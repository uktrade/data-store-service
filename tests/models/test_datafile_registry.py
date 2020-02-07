import pandas as pd

from app.constants import DatafileState
from app.db.models.internal import DatafileRegistryModel
from tests.utils import assert_dfs_equal_ignore_dtype

entry1 = {
    'source': 'source1',
    'file_name': 'test_file.zip',
    'state': DatafileState.PROCESSING.value,
    'error_message': None,
}
entry2 = {
    'source': 'source1',
    'file_name': 'test_file.zip',
    'state': DatafileState.FAILED.value,
    'error_message': 'error',
}
entry3 = {
    'source': 'source1',
    'file_name': 'test_file_2.zip',
    'state': DatafileState.PROCESSED.value,
    'error_message': None,
}
entry4 = {
    'source': 'source1',
    'file_name': 'test_file_3.zip',
    'state': DatafileState.PROCESSED.value,
    'error_message': None,
}
entry5 = {
    'source': 'source2',
    'file_name': 'test_file.zip',
    'state': DatafileState.PROCESSED.value,
    'error_message': None,
}
entry6 = {
    'source': 'source2',
    'file_name': 'test_file_4.zip',
    'state': DatafileState.IGNORED.value,
    'error_message': None,
}


def test_get_dataframe(app_with_db):
    # add data_file entry to DB
    row, _ = DatafileRegistryModel.get_update_or_create(**entry1)
    expected_df = pd.DataFrame([entry1], index=[row.id])
    expected_df.index.name = 'id'
    df1 = DatafileRegistryModel.get_dataframe()
    assert_dfs_equal_ignore_dtype(
        df1[['source', 'file_name', 'state', 'error_message']], expected_df
    )

    # update data_file entry
    DatafileRegistryModel.get_update_or_create(**entry2)
    expected_df = pd.DataFrame([entry2], index=[row.id])
    expected_df.index.name = 'id'
    df2 = DatafileRegistryModel.get_dataframe()
    assert_dfs_equal_ignore_dtype(
        df2[['source', 'file_name', 'state', 'error_message']], expected_df
    )
    assert df2.loc[1, 'created_timestamp'] < df2.loc[1, 'updated_timestamp']


def test_get_processed_or_ignored_datafile_names(app_with_db):
    DatafileRegistryModel.get_update_or_create(**entry1)
    DatafileRegistryModel.get_update_or_create(**entry2)
    DatafileRegistryModel.get_update_or_create(**entry3)
    DatafileRegistryModel.get_update_or_create(**entry4)
    DatafileRegistryModel.get_update_or_create(**entry5)
    DatafileRegistryModel.get_update_or_create(**entry6)

    processed_or_ignored_dfs = DatafileRegistryModel.get_processed_or_ignored_datafiles()

    assert processed_or_ignored_dfs == {
        'source1': [entry3['file_name'], entry4['file_name']],
        'source2': [entry5['file_name'], entry6['file_name']],
    }


def test_get_processed_or_ignored_datafile_names_one_data_source(app_with_db):
    DatafileRegistryModel.get_update_or_create(**entry1)
    DatafileRegistryModel.get_update_or_create(**entry2)
    DatafileRegistryModel.get_update_or_create(**entry3)
    DatafileRegistryModel.get_update_or_create(**entry4)
    DatafileRegistryModel.get_update_or_create(**entry5)
    DatafileRegistryModel.get_update_or_create(**entry6)

    processed_or_ignored_dfs = DatafileRegistryModel.get_processed_or_ignored_datafiles('source1')

    assert processed_or_ignored_dfs == {'source1': [entry3['file_name'], entry4['file_name']]}

    processed_or_ignored_dfs = DatafileRegistryModel.get_processed_or_ignored_datafiles('source2')

    assert processed_or_ignored_dfs == {'source2': [entry5['file_name'], entry6['file_name']]}
