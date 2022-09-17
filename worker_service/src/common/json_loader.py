"""Implements utilities related to JSON"""

import json


class RecursiveNamespace:
  """Implements namespace to map the dict"""

  @staticmethod
  def map_entry(entry):
    """Maps the entry into recusrsive namespace"""
    if isinstance(entry, dict):
      return RecursiveNamespace(**entry)
    return entry

  def __init__(self, **kwargs):
    for key, val in kwargs.items():
      if isinstance(val, dict):
        setattr(self, key, RecursiveNamespace(**val))
      elif isinstance(val, list):
        setattr(self, key, list(map(RecursiveNamespace.map_entry, val)))
      else:
        setattr(self, key, val)
    
  @staticmethod
  def _to_dict(val):
    if isinstance(val, RecursiveNamespace):
      return val.to_dict()
    elif isinstance(val, list):
      return list(map(RecursiveNamespace._to_dict, val))
    else:
      return val
  
  def to_dict(self):
    updated_dict = {}
    print(vars(self))
    for key, val in vars(self).items():
      updated_dict[key] = RecursiveNamespace._to_dict(val)
    return updated_dict


class JsonLoader:
    """Implements the loader to return the custom pytho object"""

    @staticmethod
    def loads(json_string):
        """Loads the JSON string"""
        return json.loads(json_string, object_hook=RecursiveNamespace.map_entry)
