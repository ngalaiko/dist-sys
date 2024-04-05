use std::collections::{HashMap, HashSet};

use crate::ids;

#[derive(Debug, Default)]
pub struct Topology(HashMap<ids::NodeId, Vec<ids::NodeId>>);

impl From<&HashMap<ids::NodeId, Vec<ids::NodeId>>> for Topology {
    fn from(topology: &HashMap<ids::NodeId, Vec<ids::NodeId>>) -> Self {
        let mut topology = topology.clone();
        for neighbors in topology.values_mut() {
            neighbors.sort();
        }
        Self(topology)
    }
}

impl Topology {
    /// Next returns id of nodes where this node should broadcast to.
    pub fn next(&self, node_id: ids::NodeId) -> Vec<ids::NodeId> {
        // find all cycles in the graph in a deterministic way
        let mut cycles = vec![];
        let mut visited = HashSet::new();
        let mut all_nodes = self.0.keys().copied().collect::<Vec<_>>();
        all_nodes.sort();
        let Some(start_node) = all_nodes.first().copied() else {
            return vec![];
        };
        let mut stack = vec![(start_node, vec![start_node])];
        while let Some((node_id, path)) = stack.pop() {
            visited.insert(node_id);
            for neighbor in self.get_neighbors(&node_id) {
                if path.contains(&neighbor) {
                    // cycle found
                    let start = path.iter().position(|id| id == &neighbor).unwrap();
                    cycles.push(path[start..].to_vec());
                } else if !visited.contains(&neighbor) {
                    let mut path = path.clone();
                    path.push(neighbor);
                    stack.push((neighbor, path));
                }
            }
        }

        // remove cycles that are contained in other cycles
        for cycle in cycles.clone() {
            if cycles
                .iter()
                .any(|c| c != &cycle && Self::contains_cycle(c, &cycle))
            {
                cycles.retain(|c| c != &cycle);
            }
        }

        if cycles.is_empty() {
            // no cycles => broadcast to all neighbors
            self.get_neighbors(&node_id)
        } else {
            cycles
                .into_iter()
                .flat_map(|c| {
                    // skip cycles that do not contain the node
                    if let Some(position) = c.iter().position(|&id| id == node_id) {
                        // broadcast next in the cycle
                        match c.len() {
                            1 => vec![],
                            2 => {
                                if position == 0 {
                                    vec![c[1]]
                                } else {
                                    vec![c[0]]
                                }
                            }
                            _ => {
                                if position == 0 {
                                    vec![c[1]]
                                } else if position == c.len() - 1 {
                                    vec![c[0]]
                                } else {
                                    vec![c[position + 1]]
                                }
                            }
                        }
                    } else {
                        vec![]
                    }
                })
                .fold(Vec::new(), |mut acc, id| {
                    if !acc.contains(&id) {
                        acc.push(id);
                    }
                    acc
                })
        }
    }

    fn get_neighbors(&self, node_id: &ids::NodeId) -> Vec<ids::NodeId> {
        self.0.get(node_id).cloned().unwrap_or_default()
    }

    /// checks if container containes the slice
    /// checks for both slice as is, and for the reversed slice
    /// handles circular cases too
    fn contains_cycle<T>(container: &[T], slice: &[T]) -> bool
    where
        T: PartialEq,
    {
        for s in slice {
            if !container.contains(s) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_cycle() {
        let container = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let windows = [
            [1, 2, 3],
            [2, 3, 4],
            [3, 4, 5],
            [4, 5, 6],
            [5, 6, 7],
            [6, 7, 8],
            [7, 8, 9],
            [8, 9, 1],
            [9, 1, 2],
        ];
        let reversed = container.iter().rev().copied().collect::<Vec<_>>();
        for window in windows.iter() {
            let reversed_window = window.iter().rev().copied().collect::<Vec<_>>();
            assert!(Topology::contains_cycle(&container, window));
            assert!(Topology::contains_cycle(&container, &reversed_window));
            assert!(Topology::contains_cycle(&reversed, &reversed_window));
        }
    }

    #[test]
    fn not_existing() {
        let topology = Topology::default();
        assert!(topology.next(1.into()).is_empty());
    }

    #[test]
    fn no_neighbors() {
        /*
         *   1→2
         */
        let mut topology = HashMap::<ids::NodeId, Vec<ids::NodeId>>::new();
        topology.insert(1.into(), vec![2.into()]);
        let topology = Topology::from(&topology);

        assert_eq!(topology.next(1.into()), &[2.into()]);
        assert!(topology.next(2.into()).is_empty());
    }

    #[test]
    fn one_neighbor() {
        /*
         *   1↔2
         */
        let mut topology = HashMap::<ids::NodeId, Vec<ids::NodeId>>::new();
        topology.insert(1.into(), vec![2.into()]);
        topology.insert(2.into(), vec![1.into()]);
        let topology = Topology::from(&topology);

        assert_eq!(topology.next(1.into()), [2.into()]);
        assert_eq!(topology.next(2.into()), [1.into()]);
    }

    #[test]
    fn two_neighbors() {
        /*
         *   1↔2
         *   ↕ ↕
         *   3↔4
         */
        let mut topology = HashMap::<ids::NodeId, Vec<ids::NodeId>>::new();
        topology.insert(1.into(), vec![2.into(), 3.into()]);
        topology.insert(2.into(), vec![1.into(), 4.into()]);
        topology.insert(3.into(), vec![1.into(), 4.into()]);
        topology.insert(4.into(), vec![2.into(), 3.into()]);
        let topology = Topology::from(&topology);

        assert_eq!(topology.next(1.into()), [3.into()]);
        assert_eq!(topology.next(2.into()), [1.into()]);
        assert_eq!(topology.next(3.into()), [4.into()]);
        assert_eq!(topology.next(4.into()), [2.into()]);
    }

    #[test]
    fn two_neighbors_2() {
        /*
         *   0↔1↔2
         *   ↕ ↕
         *   3↔4
         */
        let mut topology = HashMap::<ids::NodeId, Vec<ids::NodeId>>::new();
        topology.insert(0.into(), vec![1.into(), 3.into()]);
        topology.insert(1.into(), vec![0.into(), 2.into(), 4.into()]);
        topology.insert(2.into(), vec![1.into()]);
        topology.insert(3.into(), vec![0.into(), 4.into()]);
        topology.insert(4.into(), vec![1.into(), 3.into()]);
        let topology = Topology::from(&topology);

        assert_eq!(topology.next(0.into()), [3.into()]);
        assert_eq!(topology.next(1.into()), [0.into(), 2.into()]);
        assert_eq!(topology.next(2.into()), [1.into()]);
        assert_eq!(topology.next(3.into()), [4.into()]);
        assert_eq!(topology.next(4.into()), [1.into()]);
    }

    #[test]
    fn three_neighbors() {
        /*
         *   1↔2↔3
         *   ↕ ↕ ↕
         *   4↔5↔6
         */
        let mut topology = HashMap::<ids::NodeId, Vec<ids::NodeId>>::new();
        topology.insert(1.into(), vec![2.into(), 4.into()]);
        topology.insert(2.into(), vec![1.into(), 3.into(), 5.into()]);
        topology.insert(3.into(), vec![2.into(), 6.into()]);
        topology.insert(4.into(), vec![1.into(), 5.into()]);
        topology.insert(5.into(), vec![2.into(), 4.into(), 6.into()]);
        topology.insert(6.into(), vec![3.into(), 5.into()]);
        let topology = Topology::from(&topology);

        assert_eq!(topology.next(1.into()), [4.into()]);
        assert_eq!(topology.next(2.into()), [1.into()]);
        assert_eq!(topology.next(3.into()), [2.into()]);
        assert_eq!(topology.next(4.into()), [5.into()]);
        assert_eq!(topology.next(5.into()), [6.into()]);
        assert_eq!(topology.next(6.into()), [3.into()]);
    }

    #[test]
    fn four_neighbors() {
        /*
         *   1↔2↔3
         *   ↕ ↕ ↕
         *   4↔5↔6
         *   ↕ ↕ ↕
         *   7↔8↔9
         */
        let mut topology = HashMap::<ids::NodeId, Vec<ids::NodeId>>::new();
        topology.insert(1.into(), vec![2.into(), 4.into()]);
        topology.insert(2.into(), vec![1.into(), 3.into(), 5.into()]);
        topology.insert(3.into(), vec![2.into(), 6.into()]);
        topology.insert(4.into(), vec![1.into(), 5.into(), 7.into()]);
        topology.insert(5.into(), vec![2.into(), 4.into(), 6.into(), 8.into()]);
        topology.insert(6.into(), vec![3.into(), 5.into(), 9.into()]);
        topology.insert(7.into(), vec![4.into(), 8.into()]);
        topology.insert(8.into(), vec![5.into(), 7.into(), 9.into()]);
        topology.insert(9.into(), vec![6.into(), 8.into()]);
        let topology = Topology::from(&topology);

        assert_eq!(topology.next(1.into()), [4.into()]);
        assert_eq!(topology.next(2.into()), [1.into(), 3.into()]);
        assert_eq!(topology.next(3.into()), [6.into()]);
        assert_eq!(topology.next(4.into()), [7.into()]);
        assert_eq!(topology.next(5.into()), [2.into()]);
        assert_eq!(topology.next(6.into()), [5.into()]);
        assert_eq!(topology.next(7.into()), [8.into()]);
        assert_eq!(topology.next(8.into()), [9.into()]);
        assert_eq!(topology.next(9.into()), [6.into()]);
    }
}
