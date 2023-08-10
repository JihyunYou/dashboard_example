class RequestBody:
    def __init__(
            self,
            from_date,
            to_date,
            metrics,
            dimensions,
            sort_by_desc
    ):
        self.from_date = from_date
        self.to_date = to_date
        self.metrics = metrics
        self.dimensions = dimensions
        self.sort_by_desc = sort_by_desc
