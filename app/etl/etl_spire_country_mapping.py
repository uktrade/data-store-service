from app.etl.etl_rebuild_schema import RebuildSchemaPipeline


class SPIRECountryMappingsPipeline(RebuildSchemaPipeline):

    data_column_types = [
        ('country', 'text'),
        ('mapping', 'text'),
    ]
    dataset = 'country_mappings'
    organisation = 'spire'
    schema = 'dit.spire'
    table_name = 'spire_country_mappings'
