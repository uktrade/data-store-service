from data_engineering.common.views import ac, base, json_error


class PipelinePaginatedListView(base.PaginatedListView):
    pipeline_column_types = None
    decorators = [ac.authorization_required, ac.authentication_required, json_error]

    def get_select_clause(self):
        if not self.pipeline_column_types:
            raise NotImplementedError('Pipeline column types required')
        return ','.join([field for field, _ in self.pipeline_column_types])

    def get_from_clause(self):
        if not self.model:
            raise NotImplementedError('Model required')
        return self.model.get_fq_table_name()
