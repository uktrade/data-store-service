from data_engineering.common.views import ac, base, json_error


class PipelinePaginatedListView(base.PaginatedListView):
    pipeline_column_types = None
    decorators = [json_error, ac.authentication_required, ac.authorization_required]

    def get_fields(self):
        if not self.pipeline_column_types:
            raise NotImplementedError('Pipeline column types required')
        return [field for field, _ in self.pipeline_column_types]
