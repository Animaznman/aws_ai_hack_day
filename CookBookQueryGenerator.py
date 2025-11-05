from typing import Tuple
from aperturedb.CommonLibrary import create_connector, execute_query
from aperturedb.QueryGenerator import QueryGenerator
from aperturedb.types import *
from aperturedb.Sources import Sources
import json

from tqdm.auto import tqdm

class CookBookQueryGenerator(QueryGenerator):
    def __init__(self, *args, **kwargs):
        print("kwargs received:", kwargs)
        super().__init__()
        assert "dishes" in kwargs, "Path to Dishes must be provided"
        with open(kwargs["dishes"]) as ins:
            self.dishes = self.dishes = json.load(ins)
            print(f"Loaded {len(self.dishes)} dishes")

    def __len__(self) -> int:
        return len(self.dishes)

    def getitem(self, idx: int) -> Tuple[Commands, Blobs]:
        record = self.dishes[idx]
        q = [
            {
                "AddImage":{
                    "_ref": 1,
                    "properties": {
                        "contributor": record["contributor"],
                        "name": record["name"],
                        "location": record["location"],
                        "cuisine": record["cuisine"],
                        "caption": record["caption"],
                        "recipe_url": record["recipe_url"],
                        "dish_id": record["dish_id"]
                    }
                }
            }
        ]
        for i, ingredient in enumerate(record["ingredients"]):
            q.append({
                "AddEntity": {
                    "_ref": 2 + i,
                    "class": "Ingredient",
                    "connect": {
                        "ref": 1
                    },
                    "properties": {
                        "Name": ingredient["Name"],
                        "other_names": ingredient.get("other_names", ""),
                        "macronutrient": ingredient.get("macronutrient", ""),
                        "micronutrient": ingredient.get("micronutrient", ""),
                        "subgroup": ingredient.get("subgroup", ""),
                        "category": ingredient.get("category", "")
                    }
                }
            })

        blob = Sources(n_download_retries=3).load_from_http_url(record["url"], validator=lambda x: True)
        return q, [blob[1]]



client = create_connector()
generator = CookBookQueryGenerator(dishes="dishes.json")
for query, blobs in tqdm(generator):
    result, response, output_blobs = execute_query(client, query, blobs)

    if result != 0:
        print(response, query)
        break