//! *What* a worker was assigned: the parts of the brief that are policy rather
//! than a lookup of the run's own facts.

/// What the run must show for its work to count, as the brief states it.
pub fn definition_of_done(test_command_configured: bool) -> Vec<String> {
    let mut definition_of_done = vec!["Commit your work to the run branch".to_owned()];
    if test_command_configured {
        definition_of_done.push("The configured test command passes".to_owned());
    }
    definition_of_done
}
