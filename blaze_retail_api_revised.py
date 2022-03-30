import posix
from sys import getallocatedblocks
import pandas as pd
import os
import posixpath
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any

# TODO modify methods to leverage the make_get_requests/get_all_pages methods


class BlazeRetailAPIClient():
    base_url = "https://api.partners.blaze.me/api/v1/partner"

    def __init__(self,
                 partner_key: str = None,
                 Authorization: str = None,
                 *args,
                 **kwargs) -> None:
        try:
            if partner_key is None:
                self.partner_key = os.getenv('blz_partner_key')
            else:
                self.partner_key = partner_key
            if Authorization is None:
                self.Authorization = os.getenv('blz_api_key')
            else:
                self.Authorization = Authorization
        except:
            raise Exception("partner_key/Authorization is not stored in system env variables")

    def get_auth_headers(self):
        return {'partner_key': self.partner_key,
                'Authorization': self.Authorization}

    def make_get_request(self, url: str,
                         headers: dict = None,
                         params: dict = None,
                         get_all: bool = True
                         ) -> Dict[Any, Any]:
        if headers is not None:
            headers = headers
        else:
            headers = self.get_auth_headers()
        response = requests.get(url=url, params=params, headers=headers)
        # TODO Add exception by status code from Blaze API
        if response.status_code != 200:
            raise Exception(f'Error retrieving data from endpoint: {url} with status code: {response.status_code}')
        else:
            if get_all:
                return self.get_all_pages(url=url, headers=headers, params=params, data=response.json())
            else:
                return response.json()

    def get_all_pages(self, url, headers, data, params) -> Dict[Any, Any]:
        if 'start' in params.keys():
            skip = params['start'] + data['limit']
        else:
            skip = params['skip'] + data['limit']
        ttl = data.get('total')
        for i in range(skip, ttl, skip):
            if 'start' in params.keys():
                params['start'] = i
            else:
                params['skip'] = i
            data['values'] = data['values'] + (
                self.make_get_request(
                    url=url,
                    headers=headers,
                    params=params,
                    get_all=False
                    )
                ).get('values')

        return data

    def get_inventories(self) -> Dict[Any, Any]:
        url = posixpath.join(self.base_url, 'store/inventory/inventories')
        response = self.make_get_request(url=url, get_all=False)
        return response

    def get_inventory_locations(self) -> Dict[Any, Any]:
        inv = self.get_inventories().get('values')
        loc = dict()
        for x in inv:
            loc[x['name']] = x['id']
        return loc

    def get_vendors(self, skip: int = 0, get_all: bool = True) -> Dict[Any, Any]:
        url = posixpath.join(self.base_url, 'vendors')
        params = {'skip': skip}
        response = self.make_get_request(url=url, params=params)
        return response

    def get_brands(self, skip: int = 0, get_all: bool = True) -> Dict[Any, Any]:
        url = posixpath.join(self.base_url, 'store/inventory/brands')
        params = {'start': skip}
        response = self.make_get_request(url=url, params=params)
        return response

    def get_categories(self) -> Dict[Any, Any]:
        url = posixpath.join(self.base_url, 'store/inventory/categories')
        response = self.make_get_request(url=url, get_all=False)
        return response

    def get_curr_inventory(self, skip: int = 0, inventory: str = 'Safe') -> Dict[Any, Any]:
        url = posixpath.join(self.base_url, 'store/batches/quantities')
        params = {'inventoryId': self.get_inventory_locations()[inventory], 'start': skip}
        response = self.make_get_request(url=url, params=params)
        return response

    def get_employees(self, skip: int = 0, get_all: bool = True) -> Dict[Any, Any]:
        url = posixpath.join(self.base_url, 'employees')
        params = {'start': skip}
        response = self.make_get_request(url=url, params=params, get_all = get_all)
        return response
    
    def get_transactions(self, skip: int = 0, 
            start_date: str = (
                datetime.today() - timedelta(days=1)
                ).strftime('%m/%d/%Y'),
            end_date: str = datetime.today().strftime('%m/%d/%Y'),
            get_all: bool = True) -> Dict[Any, Any]:
        url = posixpath.join(self.base_url, 'transactions')
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'skip': skip
            }
        response = self.make_get_request(url=url, params=params, get_all=get_all)
        return response
    
    def get_customers(self, start_date: str = (
                datetime.today() - timedelta(days=1)
                ).strftime('%m/%d/%Y'),
            end_date: str = datetime.today().strftime('%m/%d/%Y'),
            skip: int = 0,
            get_all: bool = True):
        epoch_offset = 1000  # offset for datetime for BLAZE TS format
        startDate = int(
            datetime.strptime(
                start_date,
                '%m/%d/%Y'
                ).timestamp() * epoch_offset
            )
        endDate = int(
            datetime.strptime(
                end_date,
                '%m/%d/%Y'
                ).timestamp() * epoch_offset
            )
        url = posixpath.join(self.base_url, 'members')
        params = {
            'startDate': startDate,
            'endDate': endDate,
            'skip': skip
            }
        response = self.make_get_request(url=url, params=params, get_all=get_all)
        return response
    
    def get_purchase_orders(self, start_date: str = (
                datetime.today() - timedelta(days=1)
                ).strftime('%m/%d/%Y'),
            end_date: str = datetime.today().strftime('%m/%d/%Y'),
            skip: int = 0,
            get_all: bool = True):
        url = posixpath.join(self.base_url, 'purchaseorders/list')
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'start': skip
            }
        response = self.make_get_request(url=url, params=params, get_all=get_all)
        return response


if __name__ == '__main__':
    client = BlazeRetailAPIClient()
    # vendors = client.get_vendors()
    # brands = client.get_brands()
    # categories = client.get_categories()
    # curr_inv = client.get_curr_inventory()
    # txns = client.get_transactions()
    pos = client.get_purchase_orders(start_date='03/01/2022', end_date='03/15/2022')
    v = pd.json_normalize(vendors.get('values'))
    # inventories = client.get_inventories()
    # df = pd.json_normalize(inventories)
    # b = blaze_retail_api()
    # po = b.get_po_line_items()
    # t = b.get_transactions()
