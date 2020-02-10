import inspect
from collections import namedtuple, OrderedDict

from datatools.io.datafile_provider import DatafileProvider
from datatools.io.storage import StorageFactory
from flask import current_app as flask_app
from tqdm import tqdm

from app.constants import DatafileState
from app.db.models.internal import DatafileRegistryModel

PipelineConfig = namedtuple('PipelineConfig', 'pipeline sub_directory')


class Manager:
    """ Manages several clean pipelines and one storage instance
    """

    def __init__(self, storage=None, dbi=None):
        self.storage = self._cast_to_storage(storage)
        self.dbi = dbi
        self._pipelines = OrderedDict()

    def _to_pipeline_id(self, pipeline):
        if type(pipeline) == str:
            return pipeline
        return pipeline.id

    @classmethod
    def _cast_to_storage(cls, storage):
        """ Intelligently cast storage to a storage instance.

        Concretely this means a string is interpreted as a path from
        which a storage instance will be created. Otherwise the object
        is returned as is.

        Args:
            storage: string or datatools.io.Storage instance
        Returns:
            datatools.io.Storage instance
        """
        if type(storage) == str:
            return StorageFactory.create(storage)
        return storage

    def pipeline_get(self, pipeline):
        pipeline_id = self._to_pipeline_id(pipeline)
        return self._pipelines[pipeline_id]

    def pipeline_process(self, pipeline_id, progress_bar=None):
        pipeline_config = self.pipeline_get(pipeline_id)
        storage = self.storage.get_sub_storage(pipeline_config.sub_directory)
        dfp = DatafileProvider(storage)
        processed_and_ignored_files = DatafileRegistryModel.get_processed_or_ignored_datafiles(
            pipeline_id
        )
        for file_name in dfp.get_file_names():
            if file_name in processed_and_ignored_files[pipeline_id]:
                continue
            if progress_bar:
                progress_bar.set_postfix(str=file_name)
            pipeline = pipeline_config.pipeline
            try:
                file_info = next(dfp.read_files(file_name))
                DatafileRegistryModel.get_update_or_create(
                    source=pipeline.id,
                    file_name=file_info.name,
                    state=DatafileState.PROCESSING.value,
                )
                pipeline.process(file_info)
                DatafileRegistryModel.get_update_or_create(
                    source=pipeline.id,
                    file_name=file_info.name,
                    state=DatafileState.PROCESSED.value,
                )
            except Exception as e:
                DatafileRegistryModel.get_update_or_create(
                    source=pipeline.id,
                    file_name=file_info.name,
                    state=DatafileState.FAILED.value,
                    error_message=str(e),
                )
                flask_app.logger.error(f'pipeline processing failed: {e}')

    def pipeline_process_all(self):
        progress = tqdm(self._pipelines.keys())
        for pipeline_id in progress:
            progress.set_description(pipeline_id)
            self.pipeline_process(pipeline_id, progress_bar=progress)

    def pipeline_register(self, pipeline, sub_directory=None, pipeline_id=None):
        """ Register a clean pipeline for the manager to use

        Args:
            pipeline: pipeline class
                this will get instantiated with manager's dbi
            sub_directory: string
                represents sub directory relative manager's storage
                if None uses default based on pipeline's organisation
                and dataset names
            pipeline_id: string or None
                override the Pipeline's id (which is typically
                "{organisation}.{dataset}")
        Modifies:
            self._pipelines
        Returns:
            None
        """
        po = pipeline(dbi=self.dbi) if inspect.isclass(pipeline) else pipeline

        pipeline_id = pipeline_id or po.id
        if pipeline_id in self._pipelines:
            raise ValueError(f'{po.id} pipeline is already registered')
        if sub_directory is None:
            sub_directory = f'{po.organisation}/{po.dataset}'
        self._pipelines[pipeline_id] = PipelineConfig(po, sub_directory)

    def pipeline_remove(self, pipeline):
        """ Remove this pipeline.

        Args:
            pipeline: str or CleanDataPipeline instance
        """

        pipeline_id = self._to_pipeline_id(pipeline)
        del self._pipelines[pipeline_id]