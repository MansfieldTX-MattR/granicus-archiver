
import asyncio
from pathlib import Path
from dataclasses import dataclass

import click
from aiohttp import ClientSession

from .model import ClipCollection
from . import client


@dataclass
class BaseContext:
    out_dir: Path
    data_file: Path


@click.group
@click.argument(
    'out_dir',
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    '--data-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
)
@click.pass_context
def cli(ctx: click.Context, out_dir: Path, data_file: Path|None):
    if data_file is None:
        data_file = out_dir / 'data.json'
    ctx.obj = BaseContext(out_dir=out_dir, data_file=data_file)


@cli.command
@click.pass_obj
def check(obj: BaseContext):
    clips = ClipCollection.load(obj.data_file)
    client.check_all_clip_files(clips)

@cli.command
@click.pass_obj
def fix_dts(obj: BaseContext):
    out_dir, data_file = obj.out_dir, obj.data_file

    async def do_fix():
        local_clips: ClipCollection|None = None
        if data_file.exists():
            local_clips = ClipCollection.load(data_file)
        async with ClientSession() as session:
            clips = await client.get_main_data(session, out_dir)
        if local_clips is not None:
            local_keys = set(local_clips.clips.keys())
            overlap = set(clips.clips.keys()) & local_keys
            clips = clips.merge(local_clips)
            assert len(overlap)
            for key in overlap:
                local_clip = local_clips.clips[key]
                clip = clips.clips[key]
                assert local_clip.parse_data.datetime == clip.parse_data.datetime
            clips.save(data_file)

    asyncio.run(do_fix())

@cli.command
@click.option('--max-clips', type=int, required=False)
@click.option('--io-job-limit', type=int, default=8, show_default=True)
@click.pass_obj
def download(
    obj: BaseContext,
    max_clips: int|None,
    io_job_limit: int,
):
    clips = asyncio.run(client.amain(
        data_file=obj.data_file,
        out_dir=obj.out_dir,
        scheduler_limit=io_job_limit,
        max_clips=max_clips,
    ))



if __name__ == '__main__':
    cli()
