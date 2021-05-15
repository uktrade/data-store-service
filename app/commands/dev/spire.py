import click
from flask.cli import with_appcontext

from tests.fixtures.factories import (
    SPIREApplicationAmendmentFactory,
    SPIREApplicationCountryFactory,
    SPIREApplicationFactory,
    SPIREArsFactory,
    SPIREBatchFactory,
    SPIREControlEntryFactory,
    SPIRECountryGroupEntryFactory,
    SPIRECountryGroupFactory,
    SPIREEndUserFactory,
    SPIREFootnoteEntryFactory,
    SPIREFootnoteFactory,
    SPIREGoodsIncidentFactory,
    SPIREIncidentFactory,
    SPIREMediaFootnoteCountryFactory,
    SPIREMediaFootnoteDetailFactory,
    SPIREMediaFootnoteFactory,
    SPIREOglTypeFactory,
    SPIREReasonForRefusalFactory,
    SPIRERefArsSubjectFactory,
    SPIRERefCountryMappingFactory,
    SPIRERefDoNotReportValueFactory,
    SPIREReturnFactory,
    SPIREThirdPartyFactory,
    SPIREUltimateEndUserFactory,
)


@click.command('populate_spire_schema')
@with_appcontext
@click.option(
    '--batch_size',
    type=int,
    help='Optimum number of rows per object',
    default=10,
)
@click.option(
    '--min_batch_size',
    type=int,
    help='Minimum amount of rows per object',
    default=10,
)
def populate_spire_schema(min_batch_size, batch_size):
    """
    Populate spire schema
    """
    click.echo('Populating schema')
    min_batch_size = min(batch_size, min_batch_size)

    populate_country_mapping(min_batch_size, batch_size)
    populate_application(min_batch_size, batch_size)
    populate_ars(min_batch_size, batch_size)
    populate_footnotes(min_batch_size, batch_size)
    populate_misc(min_batch_size, batch_size)


def populate_country_mapping(min_batch_size, batch_size):
    click.echo('- Adding country data')
    SPIRECountryGroupEntryFactory.create_batch(size=batch_size)
    SPIRERefCountryMappingFactory.create_batch(size=batch_size)


def populate_application(min_batch_size, batch_size):
    click.echo('- Adding application data')
    SPIREBatchFactory.create_batch(size=min_batch_size)
    SPIREApplicationCountryFactory.create_batch(size=batch_size)
    SPIREApplicationFactory.create_batch(size=min_batch_size)
    SPIREApplicationAmendmentFactory.create_batch(size=batch_size)


def populate_ars(min_batch_size, batch_size):
    click.echo('- Adding ars data')
    batch = SPIREBatchFactory()
    country_group = SPIRECountryGroupFactory()
    SPIREIncidentFactory.create_batch(size=batch_size)
    goods_incidents = SPIREGoodsIncidentFactory.create_batch(size=min_batch_size, batch=batch)
    SPIREArsFactory.create_batch(size=batch_size, goods_incident__country_group=country_group)
    SPIRERefArsSubjectFactory.create_batch(size=batch_size)
    SPIREReasonForRefusalFactory.create_batch(size=batch_size, goods_incident=goods_incidents[0])
    SPIREControlEntryFactory.create_batch(size=batch_size)


def populate_footnotes(min_batch_size, batch_size):
    click.echo('- Adding footnotes data')
    SPIREMediaFootnoteFactory.create_batch(size=min_batch_size)
    SPIREMediaFootnoteDetailFactory.create_batch(size=batch_size)
    SPIREMediaFootnoteCountryFactory.create_batch(size=batch_size)
    SPIREFootnoteFactory.create_batch(size=min_batch_size)
    SPIREFootnoteEntryFactory.create_batch(size=batch_size)


def populate_misc(min_batch_size, batch_size):
    click.echo('- Adding misc data')
    SPIRERefDoNotReportValueFactory.create_batch(size=batch_size)
    SPIREEndUserFactory.create_batch(size=batch_size)
    SPIREUltimateEndUserFactory.create_batch(size=batch_size)
    SPIREOglTypeFactory.create_batch(size=batch_size)
    SPIREReturnFactory.create_batch(size=batch_size)
    SPIREThirdPartyFactory.create_batch(size=batch_size)
