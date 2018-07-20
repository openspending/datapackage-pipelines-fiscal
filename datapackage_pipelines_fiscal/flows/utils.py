from hashlib import md5

from slugify import Slugify


slugger = Slugify(separator="_", safe_chars="-.")


def extract_names(source):
    title = source['title']
    dataset_name = source.get('dataset-name', title)
    dataset_name = slugger(dataset_name).lower()
    resource_name = source.get('resource-name', dataset_name)

    return title, dataset_name, resource_name


def extract_storage_ids(source):
    owner_id = source['owner-id']
    _, dataset_name, _ = extract_names(source)
    dataset_id = '{}:{}'.format(owner_id, dataset_name)
    dataset_path = '{}/{}'.format(owner_id, dataset_name)
    dataset_name_hash = md5(dataset_name.encode('ascii')).hexdigest()[:16]
    db_table = '{}{}'.format(owner_id, dataset_name_hash)
    return dataset_id, db_table, dataset_path
