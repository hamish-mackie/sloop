use std::collections::BTreeMap;

pub(crate) fn find_cycle(graph: &BTreeMap<String, Vec<String>>) -> Option<Vec<String>> {
    fn visit(
        node: &str,
        graph: &BTreeMap<String, Vec<String>>,
        states: &mut BTreeMap<String, u8>,
        stack: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        states.insert(node.to_owned(), 1);
        stack.push(node.to_owned());
        let mut neighbors = graph.get(node).cloned().unwrap_or_default();
        neighbors.sort();
        neighbors.dedup();
        for neighbor in neighbors {
            match states.get(&neighbor).copied().unwrap_or_default() {
                0 => {
                    if let Some(cycle) = visit(&neighbor, graph, states, stack) {
                        return Some(cycle);
                    }
                }
                1 => {
                    let start = stack
                        .iter()
                        .position(|entry| entry == &neighbor)
                        .expect("visiting node is on the DFS stack");
                    let mut cycle = stack[start..].to_vec();
                    cycle.push(neighbor);
                    return Some(cycle);
                }
                _ => {}
            }
        }
        stack.pop();
        states.insert(node.to_owned(), 2);
        None
    }

    let mut states = BTreeMap::new();
    for node in graph.keys() {
        if states.get(node).copied().unwrap_or_default() == 0 {
            if let Some(cycle) = visit(node, graph, &mut states, &mut Vec::new()) {
                return Some(cycle);
            }
        }
    }
    None
}
