import functools

def once(func):
    """Функция - декоратор для первого подключения к БД"""

    @functools.wraps(func)
    def inner(*args, **kwargs):

        if not inner.called:

            result = func(*args, **kwargs)
            inner.called = True
            inner.handler = result

            return result
        else:
            return inner.handler

    deque = []
    deque.append(func.__name__)
    print(f"Декоратор вокруг {func.__name__}")
    inner.called = False
    inner.handler = None
    print(deque)
    return inner
    