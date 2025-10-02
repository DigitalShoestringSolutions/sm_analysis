from typing import Self

class Pipeline:
    def __init__(self,result):
        self.__result = result

    async def next(self, func):
        next_result = await func(self.__result)
        self.__result = next_result

    @classmethod
    def start(cls, initial_data={}) -> Self:
        return cls(initial_data)

    @property
    def result(self):
        return self.__result

