# Create asset hierarchy

# from cognite.experimental import CogniteClient
# from tqdm import tqdm
import json
import requests
import pandas as pd
import csv
import numpy as np

from typing import Tuple, Dict, Union, Callable
from time import sleep

from cognite.client.data_classes import (
    Asset, AssetList,
    Event, EventList, EventUpdate,
    FileMetadata, FileMetadataList, FileMetadataUpdate,
    LabelFilter,
    ThreeDAssetMappingList,
    ThreeDAssetMapping,
    ThreeDNodeList,
    ThreeDNode,
    EntityMatchingModel,
    ContextualizationJob, AssetUpdate
)


def parse_tag_to_asset_name(asset_tag_name, asset_name, asset_tag_len):
    if '.' not in asset_tag_name:
        if ';' in asset_tag_name:
            return ('-'.join([asset_tag_name.split('-')[asset_tag_len - 2],
                              asset_tag_name.split('-')[asset_tag_len - 1].split(';')[0]]))
        else:
            return ('-'.join(asset_tag_name.split('-')[asset_tag_len - 2:asset_tag_len]))
    else:
        return (asset_name)


def create_assets_from_attributes(attributes_dict):
    metadata_1 = {}
    for key in attributes_dict:
        # filtering for new assets
        if attributes_dict[key]['uniqueId'] not in raw_piaf_elem_df['uniqueId']:
            # filtering just assets with tags
            if 'piPointName' in attributes_dict[key].keys():
                if len(attributes_dict[key]['piPointName'].split('-')) >= 4:
                    metadata_1 = {
                        col: str(attributes_dict[key][col])
                        for col in attributes_dict[key].keys()
                        if col in ['piPointName', 'parentId', 'uniqueId', 'parentType', 'name', 'uom', 'valueType',
                                   'piPointId', 'path', 'dataReferenceConfigString', 'piPointPath', 'templateName',
                                   'piPointPathByName', 'description']
                        # if col not in ['path','dataReferenceConfigString','piPointPath','templateName','piPointPathByName','description']
                    }
                    asset_list.append(Asset(
                        name=parse_tag_to_asset_name(attributes_dict[key]['piPointName'], attributes_dict[key]['name'],
                                                     len(attributes_dict[key]['piPointName'].split('-'))),
                        external_id=attributes_dict[key]['uniqueId'], source=file, data_set_id=dsid,
                        description=attributes_dict[key]['description'],
                        parent_external_id=attributes_dict[key]['parentId'], metadata=metadata_1))
                else:
                    asset_list.append(
                        Asset(name=attributes_dict[key]['name'], external_id=attributes_dict[key]['uniqueId'],
                              source=file, data_set_id=dsid, description=attributes_dict[key]['description'],
                              parent_external_id=attributes_dict[key]['parentId'], metadata=metadata_1))
        if 'attributes' in attributes_dict[key].keys():
            # recursive function
            create_assets_from_attributes(attributes_dict[key]['attributes'])


'''
    create dictionary with assets sorted by level in the asset
    hierarchy tree to ensure creating just assets under the root
'''
assets_by_level_dict = {}


def classify_assets_for_level(root_asset: AssetList, assets_list, initial_classification_level):
    x: int = 1
    global assets_by_level_dict
    current_asset_level = initial_classification_level
    # print('start level creation')
    current_asset_level += 1
    temp_list = []
    # actual level external_ids
    actual_level_external_id_list = []
    for asset in root_asset:
        actual_level_external_id_list.append(asset.external_id)

    for asset in assets_list:
        if asset.external_id == root_id:
            assets_by_level_dict[0] = [asset]
        if asset.parent_external_id in actual_level_external_id_list:
            temp_list.append(asset)
            # print(f'adding{asset.external_id} to level {current_asset_level}')
        else:
            pass

    x = len(temp_list)
    if x != 0:
        assets_by_level_dict[current_asset_level] = temp_list
        # print(f'{current_asset_level} level dict_created')
        classify_assets_for_level(temp_list, assets_list, current_asset_level)
    else:
        pass
    return assets_by_level_dict


def parse_asset_name(asset_name):
    if '(' in asset_name and ')' in asset_name:
        open = [idx for idx, chr in enumerate(asset_name) if chr == '(']
        # print(f'len open {len(open)}')
        # print(f'{open}')
        close = [idx for idx, chr in enumerate(asset_name) if chr == ')']
        # print(f'len close {len(close)}')
        # print(f'{close}')
        if len(open) == len(close):
            for bracket_appeareance in range(0, len(open), 1):
                if bracket_appeareance == len(open) - 1:
                    # print(f'bracket_apperareance = {bracket_appeareance}')
                    text = asset_name[(open[bracket_appeareance]) + 1:close[bracket_appeareance]]
                    # print(f'text={text}')
                    if len(text.split('-')) == 2:
                        return (text)
                    elif len(text.split('-')) > 2:
                        text_split_len = len(text.split('-'))
                        return ('-'.join(text.split('-')[text_split_len - 2:text_split_len]))
                    else:
                        return (asset_name)
                else:
                    # print(f'bracket_apperareance = {bracket_appeareance}')
                    text = asset_name[(open[bracket_appeareance]) + 1:close[bracket_appeareance]]
                    # print(f'text={text}')
                    if len(text.split('-')) == 2:
                        return (text)
                    elif len(text.split('-')) > 2:
                        text_split_len = len(text.split('-'))
                        return ('-'.join(text.split('-')[text_split_len - 2:text_split_len]))
                    else:
                        pass

        else:
            return (asset_name)

    else:
        if len(asset_name.split('-')) > 2:
            asset_split_len = len(asset_name.split('-'))
            return ('-'.join(asset_name.split('-')[asset_split_len - 2:asset_split_len]))
        else:
            return (asset_name)

def handle(data, client: CogniteClient):
    ## upload raw piaf.elements table as pandas df

    # client.raw.databases.list()
    # client.raw.tables.list(db_name='piaf')
    raw_piaf_elem_df = client.raw.rows.list(db_name='piaf', table_name='elements-frade', limit=None).to_pandas()

    # Use FRADE as the only orphan (there are other orphans)
    raw_piaf_elem_df = raw_piaf_elem_df[
        (~raw_piaf_elem_df['parentId'].isnull()) | (raw_piaf_elem_df['name'] == 'FRADE')]

    # variables

    root_id = 'd4445119-cd3a-11ec-ae03-34735ae8231b'
    asset_data_set = 7192578419350119

    asset_list: AssetList = []

    # assets commons
    file = 'piaf'
    dsid = asset_data_set
    import datetime

    # dummy asset creation:
    for index, row in raw_piaf_elem_df.iterrows():

        # escape orphans except for root_asset asset
        if row['uniqueId'] != root_id and row['parentId'] not in raw_piaf_elem_df['uniqueId']:
            pass
        else:
            # root_asset asset
            if row['uniqueId'] == root_id:
                asset_list.append(Asset(name=row['name'], external_id=row['uniqueId'], source=file, data_set_id=dsid,
                                        description=row['description']))
            # rest of the assets
            else:
                asset_list.append(
                    Asset(name=parse_asset_name(row['name']), external_id=row['uniqueId'], source=file, data_set_id=dsid,
                          description=row['description'], parent_external_id=row['parentId']))

    # tags assets creation
    for index, row in raw_piaf_elem_df.iterrows():

        if row['attributes'] is np.nan:
            pass
        else:
            dict = row['attributes']
            create_assets_from_attributes(dict)

    root_asset = []
    for asset in asset_list:
        if asset.external_id == root_id:
            root_asset.append(asset)

    # creating dictionary to sort created assets into hierarchy levels
    classify_assets_for_level(root_asset, asset_list, 0)

    # asset tree creation
    for key in assets_by_level_dict.keys():
        client.assets.create_hierarchy(assets_by_level_dict[key])

    # Changing name for Power Generation Package *

    #from cognite.client.data_classes import AssetUpdate

    old_name_list = ['Power Generation Package 1', 'Power Generation Package 2', 'Power Generation Package 3',
                     'Power Generation Package 4']
    external_id_list = []

    for name in old_name_list:
        for asset in client.assets.list(name=name, limit=None):
            external_id_list.append(asset.external_id)

    name_list = ['ZAN-8000', 'ZAN-8100', 'ZAN-8200', 'ZAN-8300']
    description_list = ['Power Generation Package 1 (ZAN-8000)',
                        'Power Generation Package 2 (ZAN-8100)',
                        'Power Generation Package 3 (ZAN-8200)',
                        'Power Generation Package 4 (ZAN-8300)']
    for i in range(len(external_id_list)):
        external_id = external_id_list[i]
        name = name_list[i]
        description = description_list[i]
        my_update = AssetUpdate(external_id=external_id).name.set(name).description.set(description)
        res1 = client.assets.update(my_update)

    # Inserting 3D node tag
    tag_list = []
    update_list = []

    for asset in client.assets.list(partitions=250, limit=None):
        metadata_dict = {}
        try:
            pitag = asset.metadata['piPointName']
            raw_list = pitag.split('-')[-3:]
            for i in range(len(raw_list)):
                raw_list[i] = raw_list[i].upper()
            raw_list[1] = raw_list[1][:-1] + raw_list[1][-1].replace("I", "T")
            threed_node_tag = "-".join(raw_list)
            metadata_dict['3D node'] = threed_node_tag
            tag_list.append(threed_node_tag)
            my_update = AssetUpdate(asset.id).metadata.add(metadata_dict)
            update_list.append(my_update)
            res = client.assets.update(my_update)
            # df_match  = df_match.append(
            # {
            #    'key': key,
            #    'tag': asset.name,
            #    'generic asset name': metadata_dict
            # }, ignore_index=True)

        except:
            metadata_dict['3D node'] = asset.name
            my_update = AssetUpdate(asset.id).metadata.add(metadata_dict)
            update_list.append(my_update)

    res = client.assets.update(my_update)