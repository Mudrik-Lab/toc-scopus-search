# toc-scopus-search
**toc-scopus-search** is a script built on top of [pybliometrics](https://pybliometrics.readthedocs.io/en/stable/) for harvesting [Scopus](https://www.scopus.com/) metadata from publications in the field of theories of consciousness (toc).
## Installation
0. Make sure you have [Python](https://www.python.org/downloads/) installed.
    - The script was tested with the version 3.11.1
1. Clone or download the GitHub repository
    - If downloaded manually, unpack to the desired directory. 
2. Open Command Prompt and navigate to the folder containing the code:
```
cd path\to\the\repository
```
3. Create a python environment:
```
python -m venv venv
```
4. Activate the environment:
```
.\venv\Scripts\activate
```
5. Install script requirements:
```
pip install -r requirements.txt
```
You're all done!
## Quick start
0. Make sure the environment is activated:
```
cd path\to\the\repository
.\venv\Scripts\activate
```
1. Run the script:
```
python -m search_scopus
```
Congrats! Now you can look into the *harvest_configuration.json* file and adapt it to your needs.
