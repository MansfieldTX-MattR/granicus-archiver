from __future__ import annotations
from typing import Any, TYPE_CHECKING
from pathlib import Path
import asyncio

from yarl import URL
import click
from click_extra import extra_group as click_group

if TYPE_CHECKING:
    from ..cli import BaseContext

from ..clips.model import ClipCollection
from . import client


@click_group(name='aws')
@click.pass_obj
def cli(obj: BaseContext):
    """AWS sub-commands
    """
    pass

@cli.command()
@click.option(
    '--bucket-name',
    type=str,
    help='The bucket to use for the archive'
)
@click.option(
    '--clips-prefix',
    type=click.Path(
        path_type=Path,
    ),
)
@click.option(
    '--legistar-prefix',
    type=click.Path(
        path_type=Path,
    ),
)
@click.option(
    '--legistar-rguid-prefix',
    type=click.Path(
        path_type=Path,
    ),
)
@click.option(
    '--endpoint-url',
    type=str,
    default=None,
    help='Custom S3 endpoint URL (for S3-compatible services)',
)
@click.option(
    '--region-name',
    type=str,
    default=None,
    help='AWS region name',
)
@click.option(
    '--credentials-profile',
    type=str,
    default=None,
    show_default=True,
    help='AWS credentials profile name (from ~/.aws/credentials)',
)
@click.pass_obj
def config(
    obj: BaseContext,
    bucket_name: str|None,
    clips_prefix: Path|None,
    legistar_prefix: Path|None,
    legistar_rguid_prefix: Path|None,
    endpoint_url: str|None,
    region_name: str|None,
    credentials_profile: str|None,
):
    """Configure AWS settings
    """
    aws_config = obj.config.aws
    kw: dict[str, Any] = {
        'clips_prefix': clips_prefix,
        'legistar_prefix': legistar_prefix,
        'legistar_rguid_prefix': legistar_rguid_prefix,
    }
    kw = {k:Path(v) for k,v in kw.items() if v is not None}
    kw.update({
        'bucket_name': bucket_name,
        'credentials_profile': credentials_profile,
        's3_endpoint_url': URL(endpoint_url) if endpoint_url is not None else None,
        'region_name': region_name,
    })
    kw = {k:v for k,v in kw.items() if v is not None}
    changed = aws_config.update(**kw)
    if aws_config.bucket_name is None or not len(aws_config.bucket_name):
        bucket_name = click.prompt('Please enter the bucket name', type=str)
        if not bucket_name:
            raise click.Abort()
        aws_config.bucket_name = bucket_name
        changed = True
    if changed:
        obj.config.save(obj.config_file)
        click.echo('Updated AWS configuration')
    else:
        click.echo('No changes made')

@cli.command()
@click.option(
    '--max-clips',
    type=int,
    required=True,
    help='Maximum number of clips to upload',
)
@click.option(
    '--io-job-limit',
    type=int,
    default=8,
    show_default=True,
    help='Limit number of concurrent uploads to this amount'
)
@click.pass_obj
def upload_clips(obj: BaseContext, max_clips: int, io_job_limit: int):
    """Upload clips to AWS
    """
    clips = ClipCollection.load(obj.config.data_file)
    asyncio.run(client.upload_clips(
        clips=clips,
        config=obj.config,
        max_clips=max_clips,
        scheduler_limit=io_job_limit,
    ))


@cli.command()
@click.option(
    '--max-clips',
    type=int,
    required=True,
    help='Maximum number of items to upload',
)
@click.pass_obj
def upload_legistar(obj: BaseContext, max_clips: int):
    """Upload Legistar files to AWS
    """
    asyncio.run(client.upload_legistar(
        config=obj.config,
        max_clips=max_clips,
    ))


@cli.command()
@click.option(
    '--max-clips',
    type=int,
    required=True,
    help='Maximum number of items to upload',
)
@click.pass_obj
def upload_legistar_rguid(obj: BaseContext, max_clips: int):
    """Upload Real-Guid Legistar files to AWS
    """
    asyncio.run(client.upload_legistar_rguid(
        config=obj.config,
        max_clips=max_clips,
    ))
