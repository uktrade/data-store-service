import logging


class Manager:
    def __init__(self):
        self.datasources = []

    def register(self, datasource):
        if datasource not in self.datasources:
            self.datasources.append(datasource)

    def unregister(self, datasource):
        if datasource in self.datasources:
            self.datasources.remove(datasource)

    def unregister_all(self):
        if self.datasources:
            del self.observers[:]

    def update_datasources(self):
        for datasource in self.datasources:
            try:
                logging.info(f'updating datasource {datasource.__class__.__name__}')
                datasource.update()
            except Exception as e:
                print(e)
