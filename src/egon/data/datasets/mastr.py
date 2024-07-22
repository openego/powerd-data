"""
Download Marktstammdatenregister (MaStR) datasets unit registry.
It incorporates two different datasets:

Dump 2021-04-30
* Source: https://zenodo.org/records/10480930
* Used technologies: PV plants, wind turbines, biomass, hydro plants,
  combustion, nuclear, gsgk, storage
* Data is further processed in dataset
  :py:class:`egon.data.datasets.power_plants.PowerPlants`

Dump 2022-11-17
* Source: https://zenodo.org/records/10480958
* Used technologies: PV plants, wind turbines, biomass, hydro plants
* Data is further processed in module
  :py:mod:`egon.data.datasets.power_plants.mastr` `PowerPlants`

Todo: Finish docstring
TBD
"""

from functools import partial
from pathlib import Path
from urllib.request import urlretrieve
import os

from egon.data.datasets import Dataset
import egon.data.config

WORKING_DIR_MASTR_OLD = Path(".", "bnetza_mastr", "dump_2021-05-03")
WORKING_DIR_MASTR_NEW = Path(".", "bnetza_mastr", "dump_2022-11-17")


def _get_working_dir_mastr(target_file=None):
    """get most actual mastr working dir if hardset data not existing in in paths
        - WORKING_DIR_MASTR_OLD
        - WORKING_DIR_MASTR_NEW

    if target_file return path for it otherwise return most actual mastr working dir

    Returns
    -------
    path for most actual mastr working dir or file if exist

    """

    # already given dir options
    p_OLD = Path(".", "bnetza_mastr", "dump_2021-05-03")
    p_NEW = Path(".", "bnetza_mastr", "dump_2022-11-17")

    # collect all possible dir paths here if they exist
    dirs_to_check_for_working_dir = [p for p in [p_NEW, p_OLD] if os.path.isdir(p)]

    # get dirs to navigate more easily
    powerd_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))))
    execution_folder_dir = os.path.dirname(powerd_dir)  # execution_folder is parent of powerd-data
    bnetza_mastr_dir = os.path.join(execution_folder_dir, "bnetza_mastr")  # bnetza_mastr in execution_folder
    p_ALT = [d for d in os.listdir(bnetza_mastr_dir) if os.path.isdir(d)]  # keep directories only it they exist
    p_ALT.sort(reverse=True)  # sort from most actual to oldest

    dirs_to_check_for_working_dir += p_ALT  # check these dirs if they exist

    assert dirs_to_check_for_working_dir, \
        ("Looks like we don't have existing mastr working dirs; got "
         f"dirs_to_check_for_working_dir: {dirs_to_check_for_working_dir}")

    path_to_return = None
    if target_file:
        for dir in dirs_to_check_for_working_dir:
            fn = os.path.join(dir, target_file)
            if os.path.isfile(fn):
                path_to_return = fn
                break
    else:
        # due to dirs_to_check_for_working_dir is ordered
        # take first one
        path_to_return = dirs_to_check_for_working_dir[0]

    assert path_to_return, \
        ("Didn't find any existing mastr dir. Checking for a "
         f"specific target_file is {target_file}. target_file: {target_file}")

    return path_to_return


def download_mastr_data():
    """Download MaStR data from Zenodo"""

    def download(dataset_name, download_dir):
        print(f"Downloading dataset {dataset_name} to {download_dir} ...")
        # Get parameters from config and set download URL
        data_config = egon.data.config.datasets()[dataset_name]
        zenodo_files_url = (
            f"https://zenodo.org/record/"
            f"{data_config['deposit_id']}/files/"
        )

        files = []
        for technology in data_config["technologies"]:
            files.append(
                f"{data_config['file_basename']}_{technology}_cleaned.csv"
            )
        files.append("location_elec_generation_raw.csv")

        # Retrieve specified files
        for filename in files:
            if not os.path.isfile(filename):
                urlretrieve(
                    zenodo_files_url + filename, download_dir / filename
                )

    if not os.path.exists(WORKING_DIR_MASTR_OLD):
        WORKING_DIR_MASTR_OLD.mkdir(exist_ok=True, parents=True)
    if not os.path.exists(WORKING_DIR_MASTR_NEW):
        WORKING_DIR_MASTR_NEW.mkdir(exist_ok=True, parents=True)

    download(dataset_name="mastr", download_dir=WORKING_DIR_MASTR_OLD)
    download(dataset_name="mastr_new", download_dir=WORKING_DIR_MASTR_NEW)


mastr_data_setup = partial(
    Dataset,
    name="MastrData",
    version="0.0.2",
    dependencies=[],
    tasks=(download_mastr_data,),
)
