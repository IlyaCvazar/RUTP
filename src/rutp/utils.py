"""Арифметика 32-битных номеров с учётом циклического переноса (RFC 1982)."""
SEQ_MAX = 2**32

def seq_before(a: int, b: int) -> bool:
    """Возвращает True, если a < b в смысле циклических номеров."""
    return ((b - a) % SEQ_MAX) < (SEQ_MAX // 2)

def seq_after(a: int, b: int) -> bool:
    """Возвращает True, если a > b."""
    return seq_before(b, a)

def seq_leq(a: int, b: int) -> bool:
    """a <= b."""
    return a == b or seq_before(a, b)

def seq_geq(a: int, b: int) -> bool:
    """a >= b."""
    return a == b or seq_after(a, b)

def seq_diff(a: int, b: int) -> int:
    """Разность b - a с учётом wraparound (положительная)."""
    return (b - a) % SEQ_MAX

def seq_add(a: int, n: int) -> int:
    """Сложение по модулю."""
    return (a + n) % SEQ_MAX
