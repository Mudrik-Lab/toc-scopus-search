import datetime
import csv
import json
import shutil
from pathlib import Path
import pandas as pd
from pybliometrics.scopus import ScopusSearch, AbstractRetrieval


def _filter_by_subject_areas(abstracts, subject_areas, subject_areas_chosen):
    """ Filters abstracts by subject areas, 
    while counting how many papers were excluded per area.
    """

    # Initialise dictionary in which will be recorded counts for every excluded subject area
    areas = dict.fromkeys(subject_areas, 0)
    new_abstracts = []
    for ab in abstracts:
        # If the abstract's area is one of the chosen - include the abstract.
        if any(area.abbreviation in subject_areas_chosen for area in ab.subject_areas) :
            new_abstracts.append(ab)
        # Otherwise, increment count for the area of the abstract.
        else:
            for area in ab.subject_areas:
                areas[area.abbreviation] += 1
    return new_abstracts, areas

def _filter_by_methodologies(abstracts, methodologies):
    """ Filters abstracts by checking if any of provided methodologies 
    is in abstract, author keywords or index keywords.
    """

    if not methodologies:
        return abstracts

    new_abstracts = []
    for ab in abstracts:
        # If any of methodologies is found inside abstract or keywords - include the abstract.
        if any([
            methodology for methodology in methodologies 
            if any(
                methodology in ab_part for ab_part in filter(
                    None, 
                    (
                        ab.abstract, 
                        ab.authkeywords
                    )
                )
            )
        ]):
            new_abstracts.append(ab)
    return new_abstracts

def _retrieve_abstracts(eids):
    """ Retrieves abstracts by using Scopus Abstract Retrieval API. """

    # For every provided eid retrieve an abstract
    try:
        abstracts = [AbstractRetrieval(eid, refresh=True, view='FULL') for eid in eids]
    except Exception as err:
        raise Exception('Something went wrong while retrieving abstracts, please contact Scopus') from err
    return abstracts

def _filter_abstracts(abstracts, subject_areas, subject_areas_chosen, methodologies, filter_columns, dois):
    """ Filters abstracts by language, document type, subject areas, methodologies, dois.
    Records their counts.
    """
    
    # Initialise a dictionary for counting papers after each type of filter
    papers_count = dict.fromkeys(filter_columns, 0)
    # Record number of abstracts after main search
    papers_count[filter_columns[0]] = len(abstracts)

    # Filter by language
    abstracts = [ab for ab in abstracts if ab.language == 'eng']
    papers_count[filter_columns[1]] = len(abstracts)

    # Filter by document type
    abstracts = [ab for ab in abstracts if ab.subtype == 'ar']
    papers_count[filter_columns[2]] = len(abstracts)

    # Filter by subject area
    abstracts, areas = _filter_by_subject_areas(abstracts, subject_areas, subject_areas_chosen)
    papers_count[filter_columns[3]] = len(abstracts)

    # Filter by methodology if any were provided
    abstracts = _filter_by_methodologies(abstracts, methodologies) if methodologies else abstracts
    papers_count[filter_columns[4]] = len(abstracts)

    # Filter by DOIs
    abstracts = [ab for ab in abstracts if ab.doi not in dois]
    papers_count[filter_columns[5]] = len(abstracts)

    return abstracts, papers_count, areas

def _main_search(theory, paper, publication_years):
    """ Main search by query using Scopus Search API.
    """
    
    # If paper name was provided - search by REFTITLE, otherwise, search in title, abstract and keywords
    search_key = f'REFTITLE("{paper}")' if paper else f'TITLE-ABS-KEY("{theory}")'
    
    # Build query
    fields = {
            'topic' : search_key,
            'after_year' : ' OR '.join(f'(PUBYEAR AFT {interval[0]-1} AND PUBYEAR BEF {interval[1]+1})' for interval in publication_years)
        }
    query = ' AND '.join(fields.values())

    # Search by the query
    try:
        scopus_search = ScopusSearch(query, refresh=True, verbose=True)
    except Exception as err:
        raise Exception('Something went wrong while performing ScopusSearch, please contact Scopus') from err

    return scopus_search

def _process_abstracts(abstracts, search, search_df):
    """ Processes the abstract and records data to the dataframe.
    """
    
    search_df = search_df.copy()
    for index, ab in enumerate(abstracts):
        data = [
            # Paper Title
            ab.title if ab.title else 'NA',
            # Paper DOI
            ab.doi if ab.doi else 'NA',
            # Paper Authors
            ', '.join(filter(None, [author.indexed_name for author in ab.authors])) if ab.authors else 'NA',
            # Paper Source Title
            ab.publicationName if ab.publicationName else 'NA',
            # Paper Publication Year
            ab.coverDate[:4] if ab.coverDate else 'NA',
            # Paper Author Keywords
            '; '.join(ab.authkeywords) if ab.authkeywords else 'NA',
            # Authors Affiliations
            '; '.join(', '.join(filter(None, [aff.name, aff.city, aff.country])) for aff in ab.affiliation) if ab.affiliation else 'NA',
            # Paper Cited By
            ab.citedby_count if ab.citedby_count else 'NA',
            # Paper Link
            ab.scopus_link if ab.scopus_link else 'NA',
            # Paper Abstract
            search.results[index].description if search.results[index].description else 'NA',
            # Paper Funding Details
            ab.funding_text if ab.funding_text else 'NA',
            # Paper References
            '; '.join(filter(None, [ref.fulltext for ref in ab.references])) if ab.references else 'NA',
            # Paper Publisher
            ab.publisher if ab.publisher else 'NA',
            # Paper Abbreviated Source Title
            ab.sourcetitle_abbreviation if ab.sourcetitle_abbreviation else 'NA'
            ]
        search_df.loc[len(search_df)] = data

    return search_df


def search_by(
    config, 
    harvest_start_time, 
    theory,
    paper=None
    ):
    """ Loads parameters from config json, runs all search and processing functions.
    """

    # Load parameters from the config
    try:
        publication_years = config['Publication Years']
        subject_areas = config['Subject Areas'].keys()
        subject_areas_chosen = config['Chosen Subject Areas'].keys()
        harvest_id = config['Harvest ID']
        search_columns = config['Search Columns']
        harvest_columns = config['Harvest Columns']
        search_type_columns = config['Search Type Columns']
        notes_columns = config['Notes Columns']
        filter_columns = config['Filter Columns']
        methodologies = config['Methodologies']
        dois = config['DOIs to exclude']
    except KeyError as err:
        raise Exception(f'Please make sure there is a "{err.args[0]}" field in the configuration file') from err

    scopus_search = _main_search(theory, paper, publication_years)
    abstracts = _retrieve_abstracts(scopus_search.get_eids())
    abstracts, papers_count, areas = _filter_abstracts(abstracts, subject_areas, subject_areas_chosen, methodologies, filter_columns, dois)

    n_papers = len(abstracts)
    # Initialise dataframes for recording data.
    harvest_df = pd.DataFrame([[harvest_id, harvest_start_time]], index=range(n_papers), columns=harvest_columns)
    theory_df = pd.DataFrame([[theory, paper if paper else 'NA']], index=range(n_papers), columns=search_type_columns)
    notes_df = pd.DataFrame('', index=range(n_papers), columns=notes_columns)
    search_df = pd.DataFrame(columns=search_columns)

    search_df = _process_abstracts(abstracts, scopus_search, search_df)

    search_results_df = pd.concat([harvest_df, search_df, theory_df, notes_df], axis=1)
    
    return search_results_df, papers_count, areas

def main(path_to_json):
    """ Main
    """
    # Check the config file exists
    json_path = Path(path_to_json)
    assert json_path.is_file

    # Record datetime
    harvest_start_time = datetime.datetime.now()

    # Load the config file
    with open(json_path, 'r') as json_file:
        config = json.load(json_file)
    
    _columns = {
        'Search Columns': 14,
        'Harvest Columns': 2,
        'Search Type Columns': 2,
        'Notes Columns': 3,
        'Filter Columns': 6
        }
    # Check that _columns fields sizes are correct
    for column, size in _columns.items():
        assert len(config[column]) == size, f'Wrong size of the {column} array, expected size is {size}'

    # Load the configuration data
    try:
        theories = {theory_item['ToC']: theory_item['Key Papers'] for theory_item in config['Theories']}
        subject_areas = config['Subject Areas']
        search_columns = config['Search Columns']
        harvest_columns = config['Harvest Columns']
        search_type_columns = config['Search Type Columns']
        notes_columns = config['Notes Columns']
        filter_columns = config['Filter Columns']
    except KeyError as err:
        raise Exception(f'Please make sure there is a "{err.args[0]}" field in the configuration file') from err

    columns = [
            *harvest_columns,
            *search_columns,
            *search_type_columns,
            *notes_columns
        ]
    results_df = pd.DataFrame(columns=columns)
    counts_df = pd.DataFrame(columns=search_type_columns+filter_columns)
    areas_df = pd.DataFrame(columns=search_type_columns+list(subject_areas.values()))
    

    for theory, key_papers in theories.items():

        # Search by theory
        search_results_df, papers_count, areas = search_by(config, harvest_start_time, theory)
        # Translate subject area abbreviation to the full name.
        areas = {subject_areas[area]: count for area, count in areas.items()}
        # Add results from the search to the main dataframe.
        results_df = pd.concat([results_df, search_results_df], axis=0, ignore_index=True)
        # Add search type column names to the counts dict.
        papers_count.update(dict(zip(search_type_columns, (theory, 'NA'))))
        # Add filtering counts from the search to the count dataframe.
        counts_df = pd.concat([counts_df, pd.DataFrame([papers_count])], axis=0, ignore_index=True)
        # Add search type column names to the areas dict.
        areas.update(dict(zip(search_type_columns, (theory, 'NA'))))
        # Add areas counts from the search to the areas dataframe.
        areas_df = pd.concat([areas_df, pd.DataFrame([areas])], axis=0, ignore_index=True)

        for paper in key_papers:
            
            # Search by key paper
            search_results_df, papers_count, areas = search_by(config, harvest_start_time, theory, paper)
            # Translate subject area abbreviation to the full name.
            areas = {subject_areas[area]: count for area, count in areas.items()}
            # Add results from the search to the main dataframe.
            results_df = pd.concat([results_df, search_results_df], axis=0, ignore_index=True)
            # Add search type column names to the counts dict.
            papers_count.update(dict(zip(search_type_columns, (theory, paper))))
            # Add filtering counts from the search to the count dataframe.
            counts_df = pd.concat([counts_df, pd.DataFrame([papers_count])], axis=0, ignore_index=True)
            # Add search type column names to the areas dict.
            areas.update(dict(zip(search_type_columns, (theory, paper))))
            # Add areas counts from the search to the areas dataframe.
            areas_df = pd.concat([areas_df, pd.DataFrame([areas])], axis=0, ignore_index=True)

    # Remove DOI duplicates for the summary dataframe
    summary_df = results_df.drop_duplicates(subset=['DOI'])[search_columns+notes_columns]
    
    # Create results directory
    results_path = Path('results')
    results_path.mkdir(exist_ok=True)
    suffix = '.csv'
    # Write dataframes to csv
    summary_df.to_csv((results_path/harvest_start_time.strftime("summary_%d_%m_%Y")).with_suffix(suffix), index=False, quoting=csv.QUOTE_NONNUMERIC)
    results_df.to_csv((results_path/harvest_start_time.strftime("scopus_search_%d_%m_%Y")).with_suffix(suffix), index=False, quoting=csv.QUOTE_NONNUMERIC)
    counts_df.to_csv((results_path/harvest_start_time.strftime("counts_%d_%m_%Y")).with_suffix(suffix), index=False, quoting=csv.QUOTE_NONNUMERIC)
    areas_df.to_csv((results_path/harvest_start_time.strftime("areas_%d_%m_%Y")).with_suffix(suffix), index=False, quoting=csv.QUOTE_NONNUMERIC)

    # Copy the configuration file
    shutil.copyfile(path_to_json, results_path/f'harvest_configuration_{config["Harvest ID"]}_{harvest_start_time.strftime("%d_%m_%Y")}.json')

    harvest_end_time = datetime.datetime.now()
    delta = str(harvest_end_time-harvest_start_time)
    print(f"The harvest's done, elapsed time = {delta} hh:mm:ss")

if __name__ == '__main__':
    main('harvest_configuration.json')