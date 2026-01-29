import inspect
from tools.phase2_analysis import (
    run_change_type,
    run_population_stability,
    run_candidate_joinkey_simulation,
    run_joinhash_label_population,
    run_joinhash_parameter_population,
    run_collision_differencing,
    run_identity_collision_diagnostics,
)

fns = [
    ("run_change_type", run_change_type.run_change_type),
    ("run_population_stability", run_population_stability.run_population_stability),
    ("run_candidate_joinkey_simulation", run_candidate_joinkey_simulation.run_candidate_joinkey_simulation),
    ("run_joinhash_label_population", run_joinhash_label_population.run_joinhash_label_population),
    ("run_joinhash_parameter_population", run_joinhash_parameter_population.run_joinhash_parameter_population),
    ("run_collision_differencing", run_collision_differencing.run_collision_differencing),
    ("run_identity_collision_diagnostics", run_identity_collision_diagnostics.run_identity_collision_diagnostics),
]

for name, fn in fns:
    print()
    print(name, inspect.signature(fn))
