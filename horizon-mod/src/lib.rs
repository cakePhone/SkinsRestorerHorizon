//! SkinsRestorer for Horizon servers: a stored skin replaces a player's
//! `textures` property at login, before the server sends the profile.
//!
//! `/skin set <name>` looks the account up on Mojang's public services on a
//! worker thread and stores its signed `textures` property. The new skin is
//! sent the next time the player logs in. `/skin clear` restores the player's
//! own skin, `/skin update` fetches the stored name again and `/skin` shows
//! what is stored. Operators may pass a player as the last argument to change
//! someone else's skin.

#![deny(unsafe_code)]

use mod_api::{
    self, Command, CommandArgumentValue, CommandSource, Config, ConfigFile, ConfigKind,
    ConfigLoadingEvent, ConfigPath, ConfigReloadingEvent, Flow, Hooks, HostSide,
    PlayerLoginProfileEvent, PlayerRef, ProfileProperty, RegisterCommandsEvent,
    RegisterConfigsEvent, RunModCommandEvent, ServerHost, ServerTickEvent, ServerTickPhase, Text,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex, RwLock,
        mpsc::{self, Receiver, Sender},
    },
    time::Duration,
};

/// The mod's config: a JSON file of the stored skins, keyed by the player's
/// 32-digit lowercase UUID.
const CONFIG_FILE: &str = "skinsrestorer/skins.json";
const DEFAULT_CONFIG: &str = "{\n  \"skins\": {}\n}\n";

/// The profile's wire limits, as the login success keeps them.
const MAX_PROPERTIES: usize = 16;
const MAX_PROPERTY_NAME: usize = 64;
const MAX_PROPERTY_VALUE: usize = 32767;
const MAX_PROPERTY_SIGNATURE: usize = 1024;

/// A skin a player chose, with the signed `textures` property Mojang returned.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSkin {
    /// The account name the skin was looked up under, for `/skin`.
    name: String,
    /// The `textures` property's base64 value.
    value: String,
    /// Its signature, when the source signed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
}

#[derive(Default, Serialize, Deserialize)]
struct SkinsFile {
    #[serde(default)]
    skins: BTreeMap<String, StoredSkin>,
}

/// A finished lookup, handed from the worker thread to the server tick.
struct FetchResult {
    player: PlayerRef,
    requested_by: Option<PlayerRef>,
    /// The player's name when the lookup started, for the requester's answer.
    target_name: Option<String>,
    /// The name looked up, for the answer.
    looked_up: String,
    result: Result<StoredSkin, String>,
}

struct SkinsRestorer {
    skins: RwLock<BTreeMap<String, StoredSkin>>,
    fetch_tx: Sender<FetchResult>,
    fetch_rx: Mutex<Receiver<FetchResult>>,
}

impl Default for SkinsRestorer {
    fn default() -> Self {
        let (fetch_tx, fetch_rx) = mpsc::channel();
        Self {
            skins: RwLock::new(BTreeMap::new()),
            fetch_tx,
            fetch_rx: Mutex::new(fetch_rx),
        }
    }
}

impl SkinsRestorer {
    /// Reads a config file into the stored skins. A malformed file keeps the
    /// last good set.
    fn load(&self, file: &ConfigFile) {
        if file.mod_id != "skinsrestorer" || file.file != CONFIG_FILE {
            return;
        }
        match serde_json::from_str::<SkinsFile>(&file.text) {
            Ok(config) => match self.skins.write() {
                Ok(mut skins) => *skins = config.skins,
                Err(poisoned) => *poisoned.into_inner() = config.skins,
            },
            Err(error) => log::warn!("SkinsRestorer: {CONFIG_FILE}: {error}"),
        }
    }

    /// Writes the stored skins back to the config file.
    fn save(&self, host: &mut dyn ServerHost) {
        let skins = match self.skins.read() {
            Ok(skins) => skins.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let text = match serde_json::to_string_pretty(&SkinsFile { skins }) {
            Ok(text) => text + "\n",
            Err(error) => {
                log::warn!("SkinsRestorer: could not write {CONFIG_FILE}: {error}");
                return;
            }
        };
        let path = match ConfigPath::new(CONFIG_FILE) {
            Ok(path) => path,
            Err(error) => {
                log::warn!("SkinsRestorer: {CONFIG_FILE}: {error}");
                return;
            }
        };
        if let Err(error) = host.config_files().write(&path, text.as_bytes()) {
            log::warn!("SkinsRestorer: could not write {CONFIG_FILE}: {error}");
        }
    }

    fn skin(&self, player: PlayerRef) -> Option<StoredSkin> {
        let skins = self.skins.read().ok()?;
        skins.get(&uuid_key(player.uuid())).cloned()
    }

    /// Starts a lookup on a worker thread; the next `server_tick` stores it.
    fn fetch(
        &self,
        player: PlayerRef,
        requested_by: Option<PlayerRef>,
        target_name: Option<String>,
        name: String,
    ) {
        let sender = self.fetch_tx.clone();
        let looked_up = name.clone();
        std::thread::spawn(move || {
            let result = fetch_skin(&name);
            let _ = sender.send(FetchResult {
                player,
                requested_by,
                target_name,
                looked_up,
                result,
            });
        });
    }

    /// Applies a finished lookup and answers the player and the requester.
    fn finish_fetches(&self, host: &mut dyn ServerHost) {
        let results: Vec<FetchResult> = match self.fetch_rx.lock() {
            Ok(receiver) => receiver.try_iter().collect(),
            Err(poisoned) => poisoned.into_inner().try_iter().collect(),
        };
        for result in results {
            match result.result {
                Ok(skin) => {
                    let key = uuid_key(result.player.uuid());
                    match self.skins.write() {
                        Ok(mut skins) => {
                            skins.insert(key, skin.clone());
                        }
                        Err(poisoned) => {
                            poisoned.into_inner().insert(key, skin.clone());
                        }
                    }
                    self.save(host);
                    let _ = result.player.tell(
                        host,
                        Text::literal(format!(
                            "Your skin is now {}. Reconnect to see it.",
                            skin.name
                        )),
                    );
                    if let Some(requester) = result.requested_by
                        && requester != result.player
                    {
                        let target = result
                            .target_name
                            .unwrap_or_else(|| "That player".to_owned());
                        let _ = requester.tell(
                            host,
                            Text::literal(format!("Set {target}'s skin to {}.", skin.name)),
                        );
                    }
                }
                Err(error) => {
                    let line = format!("Skin lookup for {} failed: {error}", result.looked_up);
                    let _ = result.player.tell(host, Text::literal(line.clone()));
                    if let Some(requester) = result.requested_by
                        && requester != result.player
                    {
                        let _ = requester.tell(host, Text::literal(line));
                    }
                }
            }
        }
    }
}

/// The config key for a player's UUID.
fn uuid_key(uuid: u128) -> String {
    format!("{uuid:032x}")
}

/// A string's length in UTF-16 code units, what the profile's wire limits count.
fn utf16_units(text: &str) -> usize {
    text.encode_utf16().count()
}

/// Whether `skin` can be sent in a login profile: a non-empty name and value
/// inside the profile's limits, and a signature inside its limit.
fn profile_skin(skin: &StoredSkin) -> bool {
    !skin.value.is_empty()
        && !skin.name.is_empty()
        && utf16_units(&skin.name) <= MAX_PROPERTY_NAME
        && utf16_units(&skin.value) <= MAX_PROPERTY_VALUE
        && skin
            .signature
            .as_deref()
            .is_none_or(|signature| utf16_units(signature) <= MAX_PROPERTY_SIGNATURE)
}

/// The `textures` property a stored skin sends.
fn textures_property(skin: &StoredSkin) -> ProfileProperty {
    ProfileProperty::new(
        "textures".to_owned(),
        skin.value.clone(),
        skin.signature.clone(),
    )
}

/// The skin a player's login should send, if the stored one fits the profile.
fn login_skin(skins: &BTreeMap<String, StoredSkin>, player: u128) -> Option<StoredSkin> {
    skins
        .get(&uuid_key(player))
        .filter(|skin| profile_skin(skin))
        .cloned()
}

/// One Mojang profile lookup: `api.mojang.com/users/profiles/minecraft/<name>`.
#[derive(Deserialize)]
struct MojangProfile {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct MojangProperty {
    name: String,
    value: String,
    #[serde(default)]
    signature: Option<String>,
}

#[derive(Deserialize)]
struct MojangSessionProfile {
    properties: Vec<MojangProperty>,
}

/// Looks an account's signed `textures` property up on Mojang's public
/// services. Runs on a worker thread; never on the server tick.
fn fetch_skin(name: &str) -> Result<StoredSkin, String> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout(Duration::from_secs(15))
        .user_agent(concat!("SkinsRestorerHorizon/", env!("CARGO_PKG_VERSION")))
        .build();
    let profile_url = format!("https://api.mojang.com/users/profiles/minecraft/{name}");
    let body = agent
        .get(&profile_url)
        .call()
        .map_err(|error| format!("Mojang profile lookup failed: {error}"))?
        .into_string()
        .map_err(|error| format!("Mojang profile answer was unreadable: {error}"))?;
    let profile: MojangProfile = serde_json::from_str(&body)
        .map_err(|error| format!("Mojang profile answer was not JSON: {error}"))?;
    let session_url = format!(
        "https://sessionserver.mojang.com/session/minecraft/profile/{}?unsigned=false",
        profile.id
    );
    let body = agent
        .get(&session_url)
        .call()
        .map_err(|error| format!("Mojang session lookup failed: {error}"))?
        .into_string()
        .map_err(|error| format!("Mojang session answer was unreadable: {error}"))?;
    let session: MojangSessionProfile = serde_json::from_str(&body)
        .map_err(|error| format!("Mojang session answer was not JSON: {error}"))?;
    let textures = session
        .properties
        .into_iter()
        .find(|property| property.name == "textures")
        .ok_or_else(|| format!("{} has no skin property", profile.name))?;
    Ok(StoredSkin {
        name: profile.name,
        value: textures.value,
        signature: textures.signature,
    })
}

/// A Minecraft account name: 3 to 16 ASCII letters, digits or underscores.
fn valid_account_name(name: &str) -> bool {
    (3..=16).contains(&name.len())
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// The player a `/skin` command changes: the `target` argument when present,
/// else the player who typed it.
fn target_player(
    source: &CommandSource,
    target: Option<PlayerRef>,
) -> Result<PlayerRef, &'static str> {
    target
        .or(match source {
            CommandSource::Player { player, .. } => Some(*player),
            _ => None,
        })
        .ok_or("Only players can change their own skin; name a target player.")
}

impl SkinsRestorer {
    /// Answers the command's source.
    fn feedback(host: &mut dyn ServerHost, line: impl Into<String>) {
        let _ = host.commands().feedback(Text::literal(line.into()));
    }

    /// `/skin set <name> [target]`: starts the lookup.
    fn command_set(
        &self,
        host: &mut dyn ServerHost,
        source: &CommandSource,
        name: String,
        target: Option<PlayerRef>,
    ) -> Flow {
        let player = match target_player(source, target) {
            Ok(player) => player,
            Err(line) => {
                Self::feedback(host, line);
                return Flow::Cancel;
            }
        };
        let requested_by = match source {
            CommandSource::Player { player, .. } => Some(*player),
            _ => None,
        };
        let target_name = player.name(host);
        self.fetch(player, requested_by, target_name.clone(), name.clone());
        Self::feedback(host, format!("Looking up {name}..."));
        if let Some(requester) = requested_by
            && requester != player
        {
            let target = target_name.unwrap_or_else(|| "that player".to_owned());
            let _ = requester.tell(
                host,
                Text::literal(format!("Looking up {name} for {target}...")),
            );
        }
        Flow::Cancel
    }

    /// `/skin clear [target]`: forgets the stored skin.
    fn command_clear(
        &self,
        host: &mut dyn ServerHost,
        source: &CommandSource,
        target: Option<PlayerRef>,
    ) -> Flow {
        let player = match target_player(source, target) {
            Ok(player) => player,
            Err(line) => {
                Self::feedback(host, line);
                return Flow::Cancel;
            }
        };
        let removed = match self.skins.write() {
            Ok(mut skins) => skins.remove(&uuid_key(player.uuid())).is_some(),
            Err(poisoned) => poisoned
                .into_inner()
                .remove(&uuid_key(player.uuid()))
                .is_some(),
        };
        if removed {
            self.save(host);
            Self::feedback(host, "Skin cleared. Reconnect to see your own skin.");
        } else {
            Self::feedback(host, "No skin is stored for that player.");
        }
        Flow::Cancel
    }

    /// `/skin update`: looks the stored name up again.
    fn command_update(&self, host: &mut dyn ServerHost, source: &CommandSource) -> Flow {
        let CommandSource::Player { player, .. } = source else {
            Self::feedback(host, "Only players can update their own skin.");
            return Flow::Cancel;
        };
        let Some(skin) = self.skin(*player) else {
            Self::feedback(host, "No skin is stored; use /skin set <name> first.");
            return Flow::Cancel;
        };
        self.fetch(*player, Some(*player), player.name(host), skin.name.clone());
        Self::feedback(host, format!("Looking up {} again...", skin.name));
        Flow::Cancel
    }

    /// `/skin`: shows the stored skin.
    fn command_show(&self, host: &mut dyn ServerHost, source: &CommandSource) -> Flow {
        let CommandSource::Player { player, .. } = source else {
            Self::feedback(host, "Only players have a skin; name a target player.");
            return Flow::Cancel;
        };
        match self.skin(*player) {
            Some(skin) => Self::feedback(host, format!("Your skin is {}.", skin.name)),
            None => Self::feedback(host, "You use your own skin. Use /skin set <name>."),
        }
        Flow::Cancel
    }
}

#[mod_api::horizon_mod]
impl Hooks for SkinsRestorer {
    fn register_configs(&self, _: HostSide<'_>, event: &mut RegisterConfigsEvent<'_>) {
        event.configs.push(
            Config::new("skinsrestorer", ConfigKind::Common, DEFAULT_CONFIG).file(CONFIG_FILE),
        );
    }

    fn config_loading(&self, _: HostSide<'_>, event: &mut ConfigLoadingEvent<'_>) {
        self.load(event.config);
    }

    fn config_reloading(&self, _: HostSide<'_>, event: &mut ConfigReloadingEvent<'_>) {
        self.load(event.config);
    }

    fn player_login_profile(&self, event: &mut PlayerLoginProfileEvent<'_>) {
        let Some(skin) = self
            .skins
            .read()
            .ok()
            .and_then(|skins| login_skin(&skins, event.player))
        else {
            return;
        };
        event
            .properties
            .retain(|property| property.name != "textures");
        event.properties.push(textures_property(&skin));
        // Keep the profile inside the wire limits even beside the player's other
        // properties: the first 16 are sent, and a skin is always one of them.
        if event.properties.len() > MAX_PROPERTIES {
            event.properties.truncate(MAX_PROPERTIES);
            if !event
                .properties
                .iter()
                .any(|property| property.name == "textures")
            {
                event.properties.pop();
                event.properties.push(textures_property(&skin));
            }
        }
    }

    fn register_commands(&self, _: &mut dyn ServerHost, event: &mut RegisterCommandsEvent<'_>) {
        for command in [
            Command::new("skin"),
            Command::new("skin set <name:word>"),
            Command::op("skin set <name:word> <target:player>", 2),
            Command::new("skin clear"),
            Command::op("skin clear <target:player>", 2),
            Command::new("skin update"),
        ] {
            event.commands.push(command);
        }
    }

    fn run_mod_command(
        &self,
        host: &mut dyn ServerHost,
        event: &mut RunModCommandEvent<'_>,
    ) -> Flow {
        if event.command.literals.first().map(String::as_str) != Some("skin") {
            return Flow::Continue;
        }
        let subcommand = event.command.literals.get(1).map(String::as_str);
        let target = match event.command.argument("target") {
            Some(CommandArgumentValue::Player(player)) => Some(*player),
            _ => None,
        };
        match subcommand {
            Some("set") => {
                let Some(CommandArgumentValue::String(name)) = event.command.argument("name")
                else {
                    Self::feedback(host, "Usage: /skin set <name> [player]");
                    return Flow::Cancel;
                };
                if !valid_account_name(name) {
                    Self::feedback(
                        host,
                        "A Minecraft name is 3 to 16 letters, digits or underscores.",
                    );
                    return Flow::Cancel;
                }
                self.command_set(host, event.source, name.clone(), target)
            }
            Some("clear") => self.command_clear(host, event.source, target),
            Some("update") => self.command_update(host, event.source),
            _ => self.command_show(host, event.source),
        }
    }

    fn server_tick(&self, host: &mut dyn ServerHost, event: &mut ServerTickEvent<'_>) {
        if matches!(event.phase, ServerTickPhase::ServerTickStart { .. }) {
            self.finish_fetches(host);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skin(name: &str, value: &str) -> StoredSkin {
        StoredSkin {
            name: name.to_owned(),
            value: value.to_owned(),
            signature: Some("signature".to_owned()),
        }
    }

    #[test]
    fn a_stored_skin_fills_the_profiles_textures_property() {
        let mut skins = BTreeMap::new();
        skins.insert(uuid_key(7), skin("Notch", "e30="));
        let found = login_skin(&skins, 7).expect("the stored skin");
        let property = textures_property(&found);
        assert_eq!(property.name, "textures");
        assert_eq!(property.value, "e30=");
        assert_eq!(property.signature.as_deref(), Some("signature"));
        assert!(login_skin(&skins, 8).is_none());
    }

    #[test]
    fn a_skin_past_the_profile_limits_is_not_sent() {
        let mut skins = BTreeMap::new();
        skins.insert(
            uuid_key(7),
            skin("Notch", &"v".repeat(MAX_PROPERTY_VALUE + 1)),
        );
        assert!(login_skin(&skins, 7).is_none());
        skins.insert(uuid_key(7), skin("Notch", "e30="));
        skins.insert(
            uuid_key(7),
            StoredSkin {
                name: "Notch".into(),
                value: "e30=".into(),
                signature: Some("s".repeat(MAX_PROPERTY_SIGNATURE + 1)),
            },
        );
        assert!(login_skin(&skins, 7).is_none());
    }

    #[test]
    fn the_uuid_key_is_a_stable_32_digit_lowercase_hex() {
        assert_eq!(uuid_key(0x1234_abcd), "0000000000000000000000001234abcd");
    }

    #[test]
    fn only_minecraft_account_names_are_looked_up() {
        assert!(valid_account_name("Notch"));
        assert!(valid_account_name("a_1"));
        assert!(!valid_account_name("ab"));
        assert!(!valid_account_name("a-b"));
        assert!(!valid_account_name(""));
        assert!(!valid_account_name(&"a".repeat(17)));
    }
}
