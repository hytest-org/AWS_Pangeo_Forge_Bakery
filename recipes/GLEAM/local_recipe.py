import pandas as pd
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
import apache_beam as beam
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr
from tempfile import TemporaryDirectory
import os

gleam_creds = dict(
    username = os.environ["GLEAM_USER"],
    password = os.environ["GLEAM_PASSWORD"],
    port = int(os.environ["GLEAM_PORT"])
    )

dates = pd.date_range("2003", "2005", freq="A")

time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)

base_url = "sftp://hydras.ugent.be/data/v3.7b/daily/{time:%Y}/Et_{time:%Y}_GLEAM_v3.7b.nc"

def make_url(time):
    return base_url.format(time=time)

pattern = FilePattern(make_url, time_concat_dim, fsspec_open_kwargs=gleam_creds)

td = TemporaryDirectory()
target_path = td.name
target_name = "output.zarr"

transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs=gleam_creds)
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        target_root=target_path,
        store_name=target_name,
        combine_dims=pattern.combine_dim_keys,
    )
)

with beam.Pipeline() as p:
    p | transforms  
