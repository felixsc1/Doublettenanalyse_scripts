# DoublettenAnalyse Scripts

## Installation

Tested with Python 3.11.7

Online installation:

create a virtual environment: `python -m venv venv`

Activate the environment: `.\venv\Scripts\Activate.ps1` (Windows)

Install the python requirements: `pip install -r requirements.txt`

Offline installation:

Download the venv folder from the repository and place it in the root folder of the project.

Modify _venv/pyenv.cfg_: There are hardcoded paths to the python installation. Replace them with the correct paths on your system (in Command Prompt type: `where python`).

Activate it with `.\venv\Scripts\Activate.ps1` (Windows)

## Usage

There are two jupyter notebooks in the root folder that provide a high-level overview of the analyses.

_organisationen_analysis.ipynb_ For Organisationen

_personen_analysis.ipynb_ For Personen

_other_analysis.ipynb_ For some additional analyses that were not directly related to Doublettenanalyse.

These can be run for example with VS Code (jupyter extension required).

Refer to the comments in the notebooks for more information. There the individual analyses are explained in more detail.
Also refer to the Wiki for Details about Expertensuchen.

### Additional notes

To load data (Expertensuchen) modify the variable "raw_data_directory" in the notebooks.

A full analysis workflow involves running the organsiationen_analyses and personen_analysis notebook twice, 
once with only_with_Geschaeftspartner = False (For analyses that involve BAKOM records and potential Mandanten), and once with only_with_Geschaeftspartner = True (For analyses that contains records ONLY with other Mandanten such as BAZL, BAFU, etc.). Note that in both cases the same files are generated and overwritten (save them in separate folders before re-executing the notebook)
