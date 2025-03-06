from egon.data.datasets import Dataset
from egon.data.datasets.scenario_path.scenario_path import (
    clean_existing_scn_path_data,
    import_network_structure,
    import_generators,
    import_loads,
    import_links,
    import_storage_units,
    import_stores,
    import_foreign,
)


def create_powerd2025():
    scn = "powerd2025"
    import_network_structure(scn)
    import_generators(scn)
    import_loads(scn)
    import_links(scn)
    import_storage_units(scn)
    import_stores(scn)
    import_foreign(scn, year=2025)

    return


def create_powerd2030():
    scn = "powerd2030"
    import_network_structure(scn)
    import_generators(scn)
    import_loads(scn)
    import_links(scn)
    import_storage_units(scn)
    import_stores(scn)
    import_foreign(scn, year=2030)

    return


def create_powerd2035():
    scn = "powerd2035"
    import_network_structure(scn)
    import_generators(scn)
    import_loads(scn)
    import_links(scn)
    import_storage_units(scn)
    import_stores(scn)
    import_foreign(scn, year=2035)

    return


class CreateIntermediateScenarios(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="scenario_path",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(
                clean_existing_scn_path_data,
                create_powerd2025,
                create_powerd2030,
                create_powerd2035,
            ),
        )
