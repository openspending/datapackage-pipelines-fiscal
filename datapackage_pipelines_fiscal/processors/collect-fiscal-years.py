from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING


def collect_years(rows, year_field, collected_years):
    for row in rows:
        try:
            collected_years.add(int(row[year_field]))
        except:
            pass
        yield row


def process_resources(res_iter, year_field):
    collected_years = set()
    first = next(res_iter)
    yield collect_years(first, year_field, collected_years)
    yield from res_iter
    yield (
        dict(year=year)
        for year in sorted(collected_years)
    )


if __name__ == '__main__':
    params, dp, res_iter = ingest()

    year_fields = list(filter(
        lambda f: f['columnType'] == 'date:fiscal-year',
        dp['resources'][0]['schema']['fields']
    ))
    if len(year_fields) == 1:
        year_field = year_fields[0]['name']
        dp['resources'].append(dict(
            name='fiscal-years',
            path='data/fiscal-years.csv',
            schema=dict(
                fields=[
                    dict(name='year', type='integer')
                ]
            )
        ))
        dp['resources'][-1][PROP_STREAMING] = True
        spew(dp, process_resources(res_iter, year_field))

    else:
        spew(dp, res_iter)
