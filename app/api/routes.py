from app.api.views import (
    dit_baci,
    dit_reference_postcodes,
    index,
    ons_postcodes,
    world_bank_bound_rates,
    world_bank_tariffs,
)

RULES = [
    ('/', index.index),
    (
        '/api/v1/get-dit-reference-postcodes/',
        dit_reference_postcodes.DitReferencePostcodeListView.as_view(
            'list_dit_reference_postcodes'
        ),
    ),
    (
        '/api/v1/get-postcode-data/',
        dit_reference_postcodes.DitReferencePostcodeView.as_view('dit_reference_postcode1'),
    ),
    (
        '/api/v1/get-dit-reference-postcode/',
        dit_reference_postcodes.DitReferencePostcodeView.as_view('dit_reference_postcode2'),
    ),
    ('/api/v1/get-ons-postcodes/', ons_postcodes.OnsPostcodeListView.as_view('list_ons_postcodes')),
    (
        '/api/v1/get-world-bank-tariffs/',
        world_bank_tariffs.WorldBankTariffTransformListView.as_view('list_world_bank_tariff'),
    ),
    (
        '/api/v1/get-world-bank-tariffs/raw/',
        world_bank_tariffs.WorldBankTariffListView.as_view('list_world_bank_tariff_raw'),
    ),
    (
        '/api/v1/get-world-bank-bound-rates/raw/',
        world_bank_bound_rates.WorldBankBoundRatesListView.as_view(
            'list_world_bank_bound_rates_raw'
        ),
    ),
    ('/api/v1/get-dit-baci-data/', dit_baci.BACIListView.as_view('list_dit_baci'),),
]
