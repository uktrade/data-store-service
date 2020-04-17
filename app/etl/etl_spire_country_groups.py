from app.etl.etl_rebuild_schema import RebuildSchemaPipeline


class SPIRECountryGroupsPipeline(RebuildSchemaPipeline):

    data_column_types = [
        ('country', 'text'),
        ('"group"', 'text'),
    ]
    dataset = 'country_groups'
    organisation = 'spire'
    schema = 'dit.spire'
    table_name = 'spire_country_groups'
