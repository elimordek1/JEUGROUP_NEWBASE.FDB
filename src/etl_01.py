# %% IMPORTS
import codecs
import sqlite3
import time
import warnings
from collections import namedtuple
from pathlib import Path
from sqlite3 import Error
from typing import Callable, Dict

import firebirdsql
import numpy as np
import pandas as pd
from pandas import ExcelFile
from pandas.api.types import infer_dtype

warnings.filterwarnings("ignore", category=UserWarning, module='pandas')
warnings.simplefilter(action='ignore', category=UserWarning)

# %%  DATABASES

database_names = ('ALL_P',
                  'all_p_holding',
                  'ALL_P_METAL_NEW',
                  'all_p_uzravi_koneba',
                  'ALLPMARKET',
                  'ELEMENT_CONSTRUCTION',
                  'EXPERT_FOR_SOLUTIONS',
                  'ZERONELECTRICS')

databases_folder_path = Path(r'D:\Bases')
databases_folder_str = r'D:\Bases'

# %%  EXCEL CONFIG FILE

config_excel_files_path = r'C:\Users\Administrator\OneDrive\app\config\excel'

# %%  COLUMNS

new_names = {
    "PROVSUM": "თანხა",
    "PROVDOP": "ვალუტის თანხა",
    "PROVVAL": "რაოდენობა",
    "ITEM": "ზომის ერთეული",
    "ITEMDOP": "ვალუტა",
    "AN_0_CREDIT": "კრედიტის კოდი 1 დონე",
    "AN_0_DEBIT": "დებეტის კოდი 1 დონე",
    "AN_1_CREDIT": "კრედიტის კოდი 2 დონე",
    "AN_1_DEBIT": "დებეტის კოდი 2 დონე",
    "AN_2_CREDIT": "კრედიტის კოდი 3 დონე",
    "AN_2_DEBIT": "დებეტის კოდი 3 დონე",
    "OPERDATE": "ოპერაციის თარიღი",
    "DEBIT_NUM": "დებეტის ანგარიში",
    "DEBIT_NAME": "დებეტის დასახელება",
    "CREDIT_NUM": "კრედიტის ანგარიში",
    "CREDIT_NAME": "კრედიტის დასახელება",
    "DEBIT_0_NAME": "დებეტის 1 დონის დასახელება",
    "DEBIT_1_NAME": "დებეტის 2 დონის დასახელება",
    "DEBIT_2_NAME": "დებეტის 3 დონის დასახელება",
    "CREDIT_0_NAME": "კრედიტის 1 დონის დასახელება",
    "CREDIT_1_NAME": "კრედიტის 2 დონის დასახელება",
    "CREDIT_2_NAME": "კრედიტის 3 დონის დასახელება",
    "COMMENT": "ოპერაციის აღწერა"}

# columns to drop in the end
drop_cols = [
    'CONCAT_DEBIT_ID_0', 'CONCAT_DEBIT_ID_1', 'CONCAT_DEBIT_ID_2',
    'CONCAT_CREDIT_ID_0', 'CONCAT_CREDIT_ID_1', 'CONCAT_CREDIT_ID_2',
    'DEBIT_0_SOURCE', 'DEBIT_1_SOURCE', 'DEBIT_2_SOURCE',
    'CREDIT_0_SOURCE', 'CREDIT_1_SOURCE', 'CREDIT_2_SOURCE']

# %%  CHARACTERS MAPPING
mapping = {"»": "წ",
           "¼": "ჭ",
           "—": "ე",
           "ª": "რ",
           "¬": "ტ",
           "²": "ყ",
           "›": "თ",
           "\xad": "უ",
           "¤": "ო",
           "¥": "პ",
           "‹": "ა",
           "«": "ს",
           "³": "შ",
           "–": "დ",
           "¯": "ფ",
           "•": "გ",
           "¿": "ჰ",
           "¾": "ჯ",
           "Ÿ": "კ",
           "¡": "ლ",
           "š": "ზ",
           "º": "ძ",
           "½": "ხ",
           "µ": "ც",
           "˜": "ვ",
           "Œ": "ბ",
           "£": "ნ",
           "¢": "მ",
           "´": "ჩ",
           "±": "ღ",
           "œ": "ი",
           "¦": "ჟ",
           "°": "ქ",
           "ў": "მ",
           "Ѕ": "ხ",
           "\x00": "ვ",
           "�": "ვ",
           "|": "ვ",
           "Ј": "ნ",
           "Њ": "ბ",
           "њ": "ი",
           "Є": "რ",
           "Ў": "ლ",
           "џ": "კ",
           "Ґ": "პ",
           "і": "შ",
           "ѕ": "ჯ",
           "є": "ძ",
           "Ї": "ფ",
           "ј": "ჭ",
           "љ": "ზ",
           "І": "ყ",
           "ї": "ჰ",
           "ґ": "ჩ"}


# %%  FUNCTIONS

def to_int(val: str) -> int:
    return int(val)


def to_float(val: str) -> float:
    return float(val)


def to_str(val: str) -> str:
    return str(val)


def load_excel_config(file_path: Path) -> ExcelFile | None:
    try:
        return pd.ExcelFile(file_path)
    except Exception as e:
        print(f"Failed to load Excel file: {e}")
        return None


def prepare_config_dict(excel_config_file: pd.ExcelFile, table_name: str) -> pd.DataFrame | None:
    try:
        return excel_config_file.parse(table_name)
    except Exception as e:
        print(f"Failed to parse table from Excel file: {e}")
        return None


def generate_converters(column_data: Dict[str, str], dtype_to_converter: Dict[str, Callable]) -> Dict[str, Callable]:
    converters_ = {}
    for col_, dtype in column_data.items():
        if dtype != 'datetime64[ns]':
            try:
                converters_[col_] = dtype_to_converter[dtype]
            except KeyError:
                print(f"Unexpected data type: {dtype}. Defaulting to string.")
                converters_[col_] = to_str
    return converters_


def create_metadata_dict(table_name: str, excel_config_path_: str):
    """
    This function creates metadata dictionary for a specific table for each database
    from the config Excel file.
    :param excel_config_path_: separate for each database
    :param table_name:
    :return: Metadata namedtuple containing dates, converters, f_to_translate_list, use_cols,
    f_dict (column names and types), and rename_dict
    """

    # Define namedtuple
    Metadata = namedtuple('Metadata',
                          ['date_columns', 'converters', 'translation_list', 'use_cols', 'column_data', 'rename_dict'])

    # metadata dataframe
    m_df = pd.read_excel(excel_config_path_, sheet_name=table_name)

    # filter where there is YES in needed column
    m_df = m_df[m_df['NEEDED_YES_NO_DEV'] == 'YES']

    # final metadata dictionary with column names and types for a specific table
    f_dict = dict(zip(m_df['RDB$FIELD_NAME'].tolist(), m_df['PD_DTYPE'].tolist()))
    f_dict_to_translate = dict(zip(m_df['RDB$FIELD_NAME'].tolist(), m_df['TRANSLATION_YES_NO'].tolist()))

    # rename columns
    rename_dict = {}
    if 'RENAME' in list(m_df.columns):
        rename_df = m_df[(m_df['RENAME'].notnull()) & (m_df['RENAME'].notna())]
        rename_dict = dict(zip(rename_df['RDB$FIELD_NAME'].tolist(), rename_df['RENAME'].tolist()))

    # filter f_dict_to_translate where there is YES in TRANSLATION_YES_NO column
    f_dict_to_translate = {k: v for k, v in f_dict_to_translate.items() if v == 'YES'}
    f_to_translate_list = list(f_dict_to_translate.keys())

    date_columns = [col for col, dtype in f_dict.items() if dtype == 'datetime64[ns]']

    # Map data types to their corresponding conversion functions
    dtype_to_converter = {
        'int': to_int,
        'int32': to_int,
        'int16': to_int,
        'int64': to_int,
        'float': to_float,
        'float32': to_float,
        'float64': to_float,
        'str': to_str
    }

    # Create converters dictionary for non-datetime data types
    converters = generate_converters(f_dict, dtype_to_converter)

    use_cols = list(converters.keys())
    use_cols.extend(date_columns)

    return Metadata(date_columns, converters, f_to_translate_list, use_cols, f_dict, rename_dict)


def get_required_analytical_tables_and_metadata(connection_) -> (dict, pd.DataFrame):
    """
    This function retrieves the required analytical tables from the database and the metadata for all tables.

    :param connection_: Connection object to the database
    :return: A tuple consisting of a dictionary of required analytical tables and a DataFrame of metadata for all tables
    """

    # required analytical tables
    required_analytical_tables_df = pd.read_sql('''
                select
                    a.SOURCE ,a.ANALITICKOD
                from ACC_AN_RELATIONS r
                         left join ACC_ANALITTYPE a on r.TYP = a.ID
                group by a.SOURCE ,a.ANALITICKOD;
    ''', connection_)

    required_analytical_tables_dict_ = required_analytical_tables_df.set_index('SOURCE').to_dict()['ANALITICKOD']
    required_analytical_tables_dict_ = dict(sorted(required_analytical_tables_dict_.items(), key=lambda item: item[0]))

    # all tables metadata
    df_all_metadata = pd.read_sql('''
                        SELECT
                        R.RDB$RELATION_NAME,
                        R.RDB$FIELD_NAME,
                        R.RDB$FIELD_SOURCE,
                        F.RDB$FIELD_LENGTH,
                        F.RDB$FIELD_TYPE,
                        F.RDB$FIELD_SCALE,
                        F.RDB$FIELD_SUB_TYPE
                    FROM
                        RDB$RELATION_FIELDS R
                            JOIN RDB$FIELDS F
                                 ON F.RDB$FIELD_NAME = R.RDB$FIELD_SOURCE
                            JOIN RDB$RELATIONS RL
                                 ON RL.RDB$RELATION_NAME = R.RDB$RELATION_NAME
                    WHERE
                            COALESCE(R.RDB$SYSTEM_FLAG, 0) = 0
                      AND
                            COALESCE(RL.RDB$SYSTEM_FLAG, 0) = 0
                      AND
                        RL.RDB$VIEW_BLR IS NULL
                    ORDER BY
                        R.RDB$RELATION_NAME,
                        R.RDB$FIELD_POSITION;
                            ''', connection_)

    # clean and trim all metadata columns
    df_all_metadata = df_all_metadata.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # change column names: replace $ with _
    df_all_metadata.columns = df_all_metadata.columns.str.replace('$', '_')

    # create a new column that concatenates the table name and the field name
    df_all_metadata['LOOKUP'] = df_all_metadata['RDB_RELATION_NAME'] + df_all_metadata['RDB_FIELD_NAME']

    return required_analytical_tables_dict_, df_all_metadata


def get_analytical_tables_names_and_ids() -> dict:
    return {"C_BANKACCAUNTS": ["KOD_STR", "NAZVANIE"],
            "C_BANKS": ["BIK", "NAME"],
            "C_CLIENTS": ["KOD_STR", "NAME"],
            "C_CONTRACTS": ["KOD", "FULL_NAME"],
            "C_GOODS": ["KOD_STR", "NAME"],
            "C_SESXI": ["KOD", "NAZVANIE"],
            "C_SKLAD": ["KOD_STR", "NAME"],
            "C_WALUTY": ["ID_STR", "NAME"],
            "FMG_BANKVARANTY": ["ID", "NAME"],
            "FMG_BUDGET": ["ID", "NAZVANIE"],
            "FMG_CATEGORY": ["KOD", "NAZVANIE"],
            "FMG_XARJTYPE": ["KOD", "NAZVANIE"],
            "K_EMPLOY": ["KOD", "FULLNAME"],
            "OS_OS": ["NUMBER", "NAME"],
            "PROJECTBASE": ["ID", "NAZVANIE"]}


def translate_unicode_to_georgian(string_: str | None, mapping_=None) -> str | None:
    """
    This function translates unicode characters to Georgian characters.
    Pass a dictionary, mapping - declared above, with unicode characters as keys and Georgian characters as values.
    :param string_:
    :param mapping_:
    :return:
    """

    if mapping_ is None:
        mapping_ = mapping
    if string_ is None:
        return None

    # translate each character using the dictionary, keeping the original character if no translation is found
    translated = ''.join(mapping_.get(char, char) for char in string_)

    # Replace multiple spaces with a single space
    return ' '.join(translated.split())


def create_config_file(database_name: str, config_folder_path: Path) -> pd.ExcelFile:
    return pd.ExcelFile(config_folder_path / f'{database_name}.xlsx')


def detect_column_data_types(df_: pd.DataFrame) -> dict:
    column_data_types = {}

    for column in df_.columns:
        dtype = infer_dtype(df_[column])
        if dtype == 'empty':
            dtype = 'object'
        elif dtype == 'integer':
            dtype = 'object'

        column_data_types[column] = dtype

    return column_data_types


def custom_error_handler(exception, replace_value="˜"):
    position = exception.start
    byte_value = exception.object[position]  # Get the byte that caused the issue.
    return (replace_value, exception.end)


def register_custom_error_handler():
    codecs.register_error('custom_error_handler', custom_error_handler)


def decode_column(entry):
    if isinstance(entry, str):
        return entry.encode('latin-1').decode('cp1251', 'custom_error_handler')
    else:
        return entry  # If it's not a string, return it as is


def decode_with_custom_error_handler(bytes_string: bytes, decode_type='cp1251'):
    return bytes_string.decode(decode_type, 'custom_error_handler')


# TODO: add db paths
def firebird_connection(db_path: str, charset_: str = 'ISO8859_1'):
    return firebirdsql.connect(
        host='localhost',
        database=db_path,
        user='SYSDBA',
        password='masterkey',
        charset=charset_,
        port=3050)


def sqlite_connection(path: Path):
    connection_ = None
    try:
        connection_ = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection_


def fill_na_in_cols(df_, side_):
    """
    This function fills NaN values in specific columns of a dataframe with 'NA'.

    AN_0_DEBIT, AN_1_DEBIT, AN_2_DEBIT,
    AN_0_CREDIT, AN_1_CREDIT, AN_2_CREDIT, DEBIT_0_SOURCE, DEBIT_1_SOURCE

    Fill in these columns with 'NA' if they are NaN.

    Parameters:
    df (pd.DataFrame): The dataframe to be processed.
    side (str): A string ('CREDIT' or 'DEBIT') to determine which columns to fill NaNs in.

    Returns:
    df (pd.DataFrame): The dataframe with 'NA' filled columns.
    """
    for i in range(3):  # Adjust this range according to your requirement
        source_col = f'{side_}_{i}_SOURCE'
        an_col = f'AN_{i}_{side_}'

        if source_col in df_.columns:
            print(f'Filling NaNs in {source_col} with NA')
            df_[source_col] = df_[source_col].fillna('NA')
        if an_col in df_.columns:
            print(f'Filling NaNs in {an_col} with NA')
            df_[an_col] = df_[an_col].fillna('NA')

    return df_


def add_analytic_name(df_, analytic_tables_dict, side_, depth_range):
    for i in range(depth_range):
        print(f'Adding analytic name for {side_} {i}')
        analytic_source_id = [(value, str(source).strip()) for value, source in zip(
            df_[f'{side_}_{i}_SOURCE'],
            df_[f'AN_{i}_{side_}']
        )]
        df_[f'{side_}_{i}_NAME'] = [
            (analytic_tables_dict.get(k).get(v) if analytic_tables_dict.get(k) is not None else None) for k, v in
            analytic_source_id]
    return df_


def encode_decode(x):
    if isinstance(x, str):
        try:
            byte_string = x.encode('latin-1')
            decoded_string = byte_string.decode('cp1251', 'ignore')
            return decoded_string.replace('\x98', 'ვ')
        except:
            return None
    else:
        return None


def mask_func(x):
    """
    Mask function for pandas df.apply() before translating.
    :param x:
    :return:
    """
    if isinstance(x, str) and \
            not x.isnumeric() and \
            not x[0].isalpha() and \
            x[0] != '-' and \
            x[0] != 0 and \
            x[0] != '0':
        return True
    return False


def move_element_to_front(lst, element):
    """
    Move the element to the front of the list.
    :param lst: list
    :param element: element to move to the front
    :return: list with the element moved to the front
    """
    if element in lst:
        return [element] + [el for el in lst if el != element]
    return lst.copy()


def is_float(x):
    try:
        float(x)
        return True
    except ValueError:
        return False


def convert_encoding(s):
    if s is not None:
        try:
            converted = s.encode('latin-1').decode('cp1251')
            print(f"'{s}' in cp1251 : {converted}")
            return converted
        except Exception as e:
            print(f"Could not decode '{s}' in cp1251. Error: {str(e)}")
            return None
    else:
        print("None")
        return None


def merge_and_rename(df_, relations_df, side_, num):
    """
    This function merges two dataframes based on a specific column and then renames the resulting columns.

    The purpose of this function is add analytical table names and respective relation ids according to depth.

    Parameters:
    df (pd.DataFrame): The main dataframe to which the relations_df will be merged.
    relations_df (pd.DataFrame): The dataframe containing the relational data to be merged onto df.
    side (str): A string that forms part of the dynamic column name for merging and renaming purposes.
    num (int): An integer that forms part of the dynamic column name for merging and renaming purposes.

    Returns:
    df (pd.DataFrame): The merged dataframe with renamed columns.

    Usage:
    x_acc_provod = merge_and_rename(x_acc_provod, x_relations, 'DEBIT', 0)
    """

    print(f'**************** {side_} START **********************')
    # print param names
    print(f'side_ = {side_}')
    print(f'num = {num}')

    # merge with relations on newly created column (CONCAT_DEBIT_ID_0, for example)
    df_ = pd.merge(df_, relations_df[['SOURCE', 'SCET_ID_DEPTH']],
                   how='left', left_on=[f'CONCAT_{side_}_ID_{num}'], right_on=['SCET_ID_DEPTH'])

    df_.rename(columns={
        'SOURCE': f'{side_}_{num}_SOURCE'
    }, inplace=True)

    df_.drop(columns=['SCET_ID_DEPTH'], inplace=True)

    return df_


def create_folders(database_name: str):
    file_formats = ['parquet', 'excel', 'csv', 'sqlite']
    folder_paths = []

    for file_format in file_formats:
        folder_path = Path.cwd() / 'data' / file_format / database_name
        folder_path.mkdir(parents=True, exist_ok=True)
        folder_paths.append(folder_path)

    return tuple(folder_paths)


# %% MAIN FUNCTION

def main(database: str):
    # CREATE FOLDERS
    folder_paths = create_folders(database_name=database)

    db_path = '/'.join([databases_folder_str, f'{database}.FDB'])
    c = firebird_connection(db_path=db_path, charset_='WIN1251')
    c.charset = 'ISO8859_1'  # to handle encoding errors - "˜": "ვ"

    # to handle encoding errors - "˜": "ვ"
    register_custom_error_handler()

    # *********************************** ACC_HOZOP ********************************************************************
    table = 'ACC_HOZOP'
    print(f'TABLE: {table}')

    dates, converters, translation, use_cols, cols_types, rename_cols = \
        create_metadata_dict(table_name=table,
                             excel_config_path_='/'.join([config_excel_files_path, f'{database}.xlsx']))

    df_acc_hozop = pd.read_sql(f'select {", ".join(use_cols)} from {table}', c)
    print(f'Number of {table} rows: {df_acc_hozop.shape[0]}'
          f'\nNumber of {table} columns: {df_acc_hozop.shape[1]}')

    # date columns
    for col in dates:
        print(f'Converting column {col} to datetime')
        df_acc_hozop[col] = df_acc_hozop[col].apply(pd.to_datetime, errors='coerce')

    # apply converters
    for col, func in converters.items():
        print(f'Applying converter function to column {col}')
        df_acc_hozop[col] = df_acc_hozop[col].apply(func)

    # fix encoding
    for col in translation:
        print('fixing column', col)
        df_acc_hozop[col] = df_acc_hozop[col].apply(decode_column)

    # translate columns
    for col in translation:
        print(f'Translating column {col}')
        df_acc_hozop[col] = df_acc_hozop[col].apply(translate_unicode_to_georgian)

    # save to parquet
    df_acc_hozop.to_parquet(folder_paths[0] / f'{database}_{table}.parquet', index=False)

    # save to SQLite
    df_acc_hozop.to_sql(table, sqlite3.connect(folder_paths[3] / f'{database}.sqlite'), if_exists='replace',
                        index=False)

    # *********************************** ACC_PROVOD *******************************************************************
    table = 'ACC_PROVOD'
    print(f'TABLE: {table}')

    dates, converters, translation, use_cols, cols_types, rename_cols = \
        create_metadata_dict(table_name=table,
                             excel_config_path_='/'.join([config_excel_files_path, f'{database}.xlsx']))

    # read the data from firebird with fetchone() and save it to a dictionary
    dict_acc_provod = {}
    for idx, col in enumerate(use_cols):
        print(f'Column: {col} - {idx + 1} of {len(use_cols)}')
        dict_acc_provod[col] = pd.read_sql(f'select {col} from {table}', c)

    # concatenate the dictionary values
    df_acc_provod_merged = pd.concat(dict_acc_provod.values(), axis=1)

    print(f'Number of {table} rows: {df_acc_provod_merged.shape[0]}'
          f'\nNumber of {table} columns: {df_acc_provod_merged.shape[1]}')

    # date columns
    for col in dates:
        print(f'Converting column {col} to datetime')
        df_acc_provod_merged[col] = df_acc_provod_merged[col].apply(pd.to_datetime, errors='coerce')

    # apply converters
    for col, func in converters.items():
        print(f'Applying converter function to column {col}')
        df_acc_provod_merged[col] = df_acc_provod_merged[col].apply(func)

    # fix encoding
    for col in translation:
        print('fixing column', col)
        df_acc_provod_merged[col] = df_acc_provod_merged[col].apply(decode_column)

    # translate columns
    for col in translation:
        print(f'Translating column {col}')
        df_acc_provod_merged[col] = df_acc_provod_merged[col].apply(translate_unicode_to_georgian)

    # rename columns
    if rename_cols:
        df_acc_provod_merged.rename(columns=rename_cols, inplace=True)

    # create masks for the columns with $ in them, apply the mask and translate
    # we are applying the translations here as well because words appear in some ID values
    # and seems that they are needed
    for col in rename_cols.values():
        print(f'Translating column {col}')
        col_mask = df_acc_provod_merged[col].apply(mask_func)
        df_acc_provod_merged.loc[col_mask, col] = df_acc_provod_merged.loc[col_mask, col].apply(
            translate_unicode_to_georgian)

    # save to parquet
    df_acc_provod_merged.to_parquet(folder_paths[0] / f'{database}_{table}.parquet', index=False)

    # save to SQLite
    df_acc_provod_merged.to_sql(table, sqlite3.connect(folder_paths[3] / f'{database}.sqlite'),
                                if_exists='replace',
                                index=False)

    # ********************************* ANALYTICAL TABLES **************************************************************
    all_analytical_tables = pd.read_sql('select * from ACC_ANALITTYPE', c)
    required_tables_dict = {k: v for k, v in zip(all_analytical_tables['SOURCE'], all_analytical_tables['ANALITICKOD'])}
    name_and_id_cols_dict = get_analytical_tables_names_and_ids()

    analytical_tables = {}
    i = 1
    for table, id_col in required_tables_dict.items():
        print(f'processing {table}, {i} of {len(required_tables_dict)}')
        # get metadata for the table

        if name_and_id_cols_dict.get(table, None) is not None:
            name_col = name_and_id_cols_dict[table][1]
            query = f'select {name_col}, {id_col} from {table}'
            print(query)
            df = pd.read_sql(query, c, dtype={name_col: str, id_col: str})

            print(f'BEFORE APPLYING TR')
            print(df.head(2))

            # fix encoding
            df[name_col] = df[name_col].apply(decode_column)
            df[id_col] = df[id_col].apply(decode_column)

            # translate columns
            df[name_col] = df[name_col].apply(translate_unicode_to_georgian)
            df[id_col] = df[id_col].apply(translate_unicode_to_georgian)

            print('*' * 50)
            print(f'AFTER APPLYING TR')
            print(df.head(2))

            df.rename(columns={name_col: 'NAME', id_col: 'ID'}, inplace=True)

            # add source column
            df['SOURCE'] = table

            # add to dictionary
            analytical_tables[table] = df

            # save to parquet
            df.to_parquet(folder_paths[0] / f'{database}_{table}.parquet', index=False)
            # print(f'{table}: table saved to parquet')
            print('*' * 50)
            print('\n')
            i += 1
            # sleep(1)
            time.sleep(1)

    # concat all analytical tables
    df_analytical_tables = pd.concat(analytical_tables.values(), ignore_index=True)

    df_analytical_tables.sort_values(by=['SOURCE', 'ID', 'NAME'], inplace=True)
    df_analytical_tables.reset_index(drop=True, inplace=True)

    # save to parquet
    df_analytical_tables.to_parquet(folder_paths[0] / f'{database}_ANALYTICAL_TABLES.parquet', index=False)

    # save to SQLite
    df_analytical_tables.to_sql('ANALYTICAL_TABLES',
                                sqlite3.connect(folder_paths[3] / f'{database}.sqlite'),
                                if_exists='replace',
                                index=False)

    # ************************************* HELPER TABLES **************************************************************
    # ANALITTYPE
    # nothing to translate in this table
    table = 'ACC_ANALITTYPE'
    print(f'processing {table}')
    df_analittype = pd.read_sql(f'select * from {table}', c)

    # **********************************************************************************************************************
    # ACC_AN_RELATIONS
    table = 'ACC_AN_RELATIONS'
    print(f'processing {table}')
    dates, converters, translation, use_cols, cols_types, rename_cols = \
        create_metadata_dict(table_name=table,
                             excel_config_path_='/'.join([config_excel_files_path, f'{database}.xlsx']))

    df_relations = pd.read_sql(f'select {", ".join(use_cols)} from {table}', c)

    # fix encoding
    for col in translation:
        print(f'fixing {col}')
        df_relations[col] = df_relations[col].apply(decode_column)

    # translate columns
    for col in translation:
        print(f'translating {col}')
        df_relations[col] = df_relations[col].dropna().apply(translate_unicode_to_georgian)

    # *******************************************************************************************************************
    # ACC_PL_SCET
    table = 'ACC_PLSCET'
    print(f'processing {table}')
    dates, converters, translation, use_cols, cols_types, rename_cols = \
        create_metadata_dict(table_name=table,
                             excel_config_path_='/'.join([config_excel_files_path, f'{database}.xlsx']))

    df_plscet = pd.read_sql(f'select {", ".join(use_cols)} from {table}', c)

    if dates:
        print('converting dates')
        for date in dates:
            print(f'converting {date}')
            df_plscet[date] = pd.to_datetime(df_plscet[date], errors='coerce')

    # fix encoding
    for col in translation:
        print(f'fixing {col}')
        df_plscet[col] = df_plscet[col].apply(decode_column)

    # translate columns
    for col in translation:
        print(f'translating {col}')
        df_plscet[col] = df_plscet[col].dropna().apply(translate_unicode_to_georgian)

    # ******************************************************************************************************************
    # Merge ACC_AN_RELATIONS with ACC_ANALITTYPE

    # begin the names of all dfs with x_
    x_acc_provod = df_acc_provod_merged.copy()
    # convert credit id and debit id to int
    x_acc_provod['CREDIT_ID'] = x_acc_provod['CREDIT_ID'].astype(int)
    x_acc_provod['DEBIT_ID'] = x_acc_provod['DEBIT_ID'].astype(int)

    # ******************************************************************************************************************
    x_plscet = df_plscet.copy()
    x_analittype = df_analittype.copy()
    x_relations = df_relations.copy()
    x_analytical_tables = df_analytical_tables.copy()
    x_analytical_tables['SOURCE'].unique()

    # name dfs
    x_plscet.name = 'x_plscet'
    x_analittype.name = 'x_analittype'
    x_relations.name = 'x_relations'
    x_analytical_tables.name = 'x_analytical_tables'
    x_acc_provod.name = 'x_acc_provod'

    # clean, trim and remove spaces from x_analytical_tables['SOURCE']
    for df in [x_plscet, x_analittype, x_relations, x_analytical_tables, x_acc_provod]:
        print(df.name)
        if 'SOURCE' in df.columns:
            print('\t', 'SOURCE')
            df['SOURCE'] = df['SOURCE'].apply(lambda x: x.strip().replace(' ', '_').upper())

    # replace errors with nans for each df
    for df in [x_plscet, x_analittype, x_relations, x_analytical_tables, x_acc_provod]:
        print(df.name)
        for col in df.columns:
            print('\t', col)
            df[col] = df[col].replace(
                ['NONE', 'None', 'none', 'nan', 'NaN', 'NAN', 'n/a', 'N/A', 'N/a', 'N/A', '', ' '],
                np.nan)

    # ********************************* !!! BEGIN MERGING OPERATIONS !!! ***********************************************
    # merge relations with analittype
    x_relations = pd.merge(x_relations, x_analittype[['SOURCE', 'ID']],
                           how='left', left_on=['TYP'], right_on=['ID'])

    x_relations.drop(columns=['ID_y'], inplace=True)
    x_relations['SOURCE'].unique()

    df_analytical_tables['SOURCE'].unique()

    # get the depth of the DEBIT relations from x_acc_provod (next columns of either debit or credit)
    x_acc_provod['CONCAT_DEBIT_ID_0'] = ['_'.join([str(x), '0']) for x in x_acc_provod['DEBIT_ID']]
    x_acc_provod['CONCAT_DEBIT_ID_1'] = ['_'.join([str(x), '1']) for x in x_acc_provod['DEBIT_ID']]
    x_acc_provod['CONCAT_DEBIT_ID_2'] = ['_'.join([str(x), '2']) for x in x_acc_provod['DEBIT_ID']]

    # get the depth of the CREDIT relations from x_acc_provod (next columns of either debit or credit)
    x_acc_provod['CONCAT_CREDIT_ID_0'] = ['_'.join([str(x), '0']) for x in x_acc_provod['CREDIT_ID']]
    x_acc_provod['CONCAT_CREDIT_ID_1'] = ['_'.join([str(x), '1']) for x in x_acc_provod['CREDIT_ID']]
    x_acc_provod['CONCAT_CREDIT_ID_2'] = ['_'.join([str(x), '2']) for x in x_acc_provod['CREDIT_ID']]

    # same for x_relations
    x_relations['SCET_ID_DEPTH'] = \
        ['_'.join([str(x), str(y)]) for x, y in zip(x_relations['SCET_ID'], x_relations['NUMINSCET'])]

    # Merge ACC_PROVOD with ACC_PLSCET
    # This will give us the names and numbers of the accounts
    # **************** DEBIT START ************************
    side = 'DEBIT'
    print(f'**************** {side} START **********************')
    x_acc_provod = pd.merge(x_acc_provod, x_plscet[['NUM', 'NAME', 'ID']],
                            how='left', left_on=[f'{side}_ID'], right_on=['ID'])

    x_acc_provod.drop(columns=['ID'], inplace=True)

    # rename name to debit name
    x_acc_provod.rename(columns={'NAME': f'{side}_NAME'}, inplace=True)

    # rename num to debit num
    x_acc_provod.rename(columns={'NUM': f'{side}_NUM'}, inplace=True)
    # **************** DEBIT END *************************

    # **************** CREDIT START **********************
    side = 'CREDIT'
    print(f'**************** {side} START **********************')
    x_acc_provod = pd.merge(x_acc_provod, x_plscet[['NUM', 'NAME', 'ID']],
                            how='left', left_on=[f'{side}_ID'], right_on=['ID'])
    x_acc_provod.drop(columns=['ID'], inplace=True)

    # rename name to credit name
    x_acc_provod.rename(columns={'NAME': f'{side}_NAME'}, inplace=True)

    # rename num to credit num
    x_acc_provod.rename(columns={'NUM': f'{side}_NUM'}, inplace=True)
    # **************** CREDIT END **********************

    x_acc_provod_01 = x_acc_provod.copy()

    x_acc_provod_01 = merge_and_rename(x_acc_provod_01, x_relations, 'DEBIT', 0)
    x_acc_provod_01 = merge_and_rename(x_acc_provod_01, x_relations, 'DEBIT', 1)
    x_acc_provod_01 = merge_and_rename(x_acc_provod_01, x_relations, 'DEBIT', 2)

    x_acc_provod_01 = merge_and_rename(x_acc_provod_01, x_relations, 'CREDIT', 0)
    x_acc_provod_01 = merge_and_rename(x_acc_provod_01, x_relations, 'CREDIT', 1)
    x_acc_provod_01 = merge_and_rename(x_acc_provod_01, x_relations, 'CREDIT', 2)

    # **************** CREDIT END *************************

    # change nan to 'NA' in x_analytical_tables['SOURCE'], x_acc_provod['DEBIT_0_SOURCE'] and x_acc_provod['AN$0_DEBIT']
    x_analytical_tables['SOURCE'] = x_analytical_tables['SOURCE'].fillna('NA')

    x_acc_provod_01 = fill_na_in_cols(x_acc_provod_01, 'DEBIT')
    x_acc_provod_01 = fill_na_in_cols(x_acc_provod_01, 'CREDIT')

    # create a mask where True indicates a row where 'SOURCE' contains a period (.)
    # because we want to remove the period and everything after it in rows where int is followed by a period
    mask_period = x_analytical_tables['ID'].str.contains('\\.')

    # apply the mask and then replace the 'SOURCE' values by splitting on period and keeping only the part before it
    x_analytical_tables.loc[mask_period, 'ID'] = x_analytical_tables.loc[mask_period, 'ID'].str.split('\\.').str[0]

    # create a dict from df_analytical_tables
    x_analytical_tables_dict = {}
    for k in x_analytical_tables['SOURCE'].unique():
        x_analytical_tables_dict[k] = x_analytical_tables.query(f'SOURCE == "{k}"').set_index('ID')['NAME'].to_dict()

    # add 'NA' to the dict
    x_analytical_tables_dict['NA'] = {'NA': 'NA'}

    x_acc_provod_01 = add_analytic_name(x_acc_provod_01, x_analytical_tables_dict, "DEBIT", 3)
    x_acc_provod_01 = add_analytic_name(x_acc_provod_01, x_analytical_tables_dict, "CREDIT", 3)

    # merge acc_provod_01 with df_acc_hozop on OPERDATE and NUMINDATE
    x_acc_provod_01['OPERDATE'] = pd.to_datetime(x_acc_provod_01['OPERDATE'], errors='coerce')

    x_acc_provod_02 = pd.merge(x_acc_provod_01, df_acc_hozop[['COMMENT', 'OPERDATE', 'NUMINDATE']], how='left',
                               left_on=['OPERDATE', 'NUMINDATE'], right_on=['OPERDATE', 'NUMINDATE']).copy()

    x_acc_provod_02.sort_values(by=['OPERDATE', 'NUMINDATE'], inplace=True)

    x_acc_provod_03 = x_acc_provod_02.drop(columns=drop_cols).copy()
    x_acc_provod_03.rename(columns=new_names, inplace=True)
    x_acc_provod_03['NUSUM'] = x_acc_provod_03['NUSUM'].fillna(0)

    # SEND TO SQLite
    x_acc_provod_03.to_sql('_X_FULL', sqlite3.connect(folder_paths[3] / f'{database}.sqlite'), if_exists='replace',
                           index=False)

    # SAVE TO PARQUET
    x_acc_provod_03.to_parquet(folder_paths[0] / f"{database}_X_FULL.parquet", index=False)

    # print done
    print(f'**************** {database} DONE **********************')


# %% IF MAIN MAIN
if __name__ == '__main__':
    # database = database_names[-1]
    # main(database)

    for db in database_names:
        print(db)
        main(db)
