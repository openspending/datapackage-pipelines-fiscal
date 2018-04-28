from datapackage_pipelines.wrapper import process
from datapackage_pipelines.generators import slugify

from datapackage_pipelines_fiscal.processors.consts import ID_COLUMN_NAME

def modify_datapackage(dp, parameters, *_):
    db_tables = parameters['db-tables']
    model = dp['model']
    field_types = dict((x['slug'], x['type']) for x in dp['resources'][-1]['schema']['fields'])

    bbg_hierarchies = {}
    bbg_dimensions = {}
    bbg_measures = {}

    # Iterate on dimensions
    for hierarchy_name, h_props in model['dimensions'].items():

        # Append to hierarchies
        hierarchy_name = slugify(hierarchy_name, separator='_')
        hierarchy = dict(
            label=hierarchy_name,
            levels=h_props['primaryKey']
        )
        bbg_hierarchies[hierarchy_name] = hierarchy

        # Get all hierarchy columns
        attributes = h_props['attributes']
        attributes = list(attributes.items())

        # Separate to codes and labels
        codes = dict(filter(lambda x: 'labelfor' not in x[1], attributes))
        labels = dict(map(lambda y: (y[1]['labelfor'], y[1]), 
                          filter(lambda x: 'labelfor' in x[1], attributes)))

        # For each code, create a babbage dimension
        for fieldname, attribute in codes.items():
            dimension_name = fieldname

            bbg_attributes = {
                fieldname: dict(
                    column='.'.join([db_tables[hierarchy_name], fieldname]),
                    label=attribute.get('title', attribute['source']),
                    type=field_types[fieldname]
                )
            }
            bbg_dimension = dict(
                attributes=bbg_attributes,
                key_attribute=fieldname,
                label=attribute.get('title'),
                join_column=[hierarchy_name+'_id', ID_COLUMN_NAME]
            )

            label = labels.get(fieldname)
            if label is not None:
                fieldname = label['source']
                attribute = label
                bbg_attributes.update({
                    fieldname: dict(
                        column='.'.join([db_tables[hierarchy_name], fieldname]),
                        label=attribute.get('title', attribute['source']),
                        type=field_types[fieldname]
                    )
                })
                bbg_dimension.update(dict(
                    label_attribute=fieldname
                ))
            bbg_dimensions[dimension_name] = bbg_dimension

    # Iterate on measures
    for measurename, measure in model['measures'].items():
        bbg_measures[measurename] = dict(
            column=measurename,
            label=measure.get('title', attribute['source']),
            type=field_types[measurename]
        )

    dp['babbageModel'] = dict(
        fact_table = db_tables[''],
        dimensions = bbg_dimensions,
        hierarchies = bbg_hierarchies,
        measures = bbg_measures
    )

    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage)