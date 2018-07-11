class SerdeException(Exception):
    def __init__(self, val=None, code=None, par=None, **kwargs):
        self.val = val
        self.code = code
        self.parent = par
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.kwargs = kwargs
        super().__init__(val, code, kwargs, par)

    def __str__(self) -> str:
        return f'{self.__class__.__name__}(Val={self.val} Code={self.code} KwArgs={self.kwargs} Par={self.parent})'

    def resolve(self):
        r = self

        while r.parent is not None and isinstance(r.parent, SerdeException):
            r = r.parent

        return r
