use nanoid::nanoid;
use std::format;

const ALPHABET: [char; 36] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
];

pub fn generate_plugin_id() -> String {
    format!("PLUG-{}", nanoid!(12, &ALPHABET))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn an_id_says_what_it_names() {
        let id = generate_plugin_id();

        assert!(id.starts_with("PLUG-"));
        assert_eq!(id.len(), "PLUG-".len() + 12);
    }

    /// Ids name files on disk and nodes on the canvas, so two of them being
    /// the same is two plugins sharing a dump.
    #[test]
    fn ids_do_not_repeat() {
        let ids: HashSet<String> = (0..1000).map(|_| generate_plugin_id()).collect();

        assert_eq!(ids.len(), 1000);
    }

    #[test]
    fn an_id_is_made_only_of_characters_a_filename_takes() {
        let id = generate_plugin_id();

        assert!(
            id.strip_prefix("PLUG-")
                .unwrap()
                .chars()
                .all(|c| ALPHABET.contains(&c)),
            "{id}"
        );
    }
}
