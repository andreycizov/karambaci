class A:
    @classmethod
    def key_fn(cls, id):
        raise TypeError('asd')

    @classmethod
    def main(cls, id):
        return cls.key_fn(id)


class B(A):
    @classmethod
    def key_fn(cls, id):
        return f'asd{id}'


x = B.main('a')

print(x)
