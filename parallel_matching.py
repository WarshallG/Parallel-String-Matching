import concurrent.futures
import math
import time
import os
from kmp import par_kmp_search
from concurrent.futures import ThreadPoolExecutor
from collections import deque

def calculate_witness(pattern, j):
    m = len(pattern)
    if j == 1:
        return 0  # WIT(1) 定义为 0
    
    for w in range(1, m - j + 2):
        if pattern[j - 1 + w - 1] != pattern[w - 1]:
            return w
    return 0  # 如果完全匹配

def process_block(pattern, start, end):
    block_result = []
    for j in range(start, end + 1):
        block_result.append((j, calculate_witness(pattern, j)))
    return block_result

def compute_witness_parallel_process(pattern, m, num_threads=4):
    wit_length = m // 2 + 1 if m % 2 == 0 else m // 2 + 2  # 奇数要多算一个
    wit = [0] * wit_length

    # 计算每块的范围
    total_tasks = wit_length - 1
    block_size = math.ceil(total_tasks / num_threads)
    blocks = [
        (i * block_size + 1, min((i + 1) * block_size, total_tasks))
        for i in range(num_threads)
    ]

    # 并行处理
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_block, pattern, start, end) for start, end in blocks]
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.extend(future.result())

    # 收集结果到 wit 数组
    for j, w in results:
        wit[j] = w

    return wit

def compute_witness_serial(pattern, m):
    wit_length = m // 2 + 1 if m % 2 == 0 else m // 2 + 2
    wit = [0] * (wit_length)

    for j in range(1, wit_length):
        if j == 1:
            wit[j] = 0  # WIT(1) 定义为 0
            continue

        for w in range(1, m - j + 2):
            if pattern[j - 1 + w - 1] != pattern[w - 1]:  # 找到第一个失配位置
                wit[j] = w
                break
        else:
            wit[j] = 0  # 完全匹配

    return wit

def match_pattern_non_period(T, P, WITNESS, n, m):
    # 非周期串的算法实现
    # 文本串分块
    block_size = m // 2
    num_blocks = math.ceil((n - m + 1) / block_size)
    blocks = [(i * block_size, min((i + 1) * block_size - 1, n - m)) for i in range(num_blocks)]

    def duel(p, q):
        j = q - p + 1
        w = WITNESS[j]
        try:
            if T[q + w - 1] != P[w - 1]:
                # WIT[q] = w  # 这里似乎并没有什么用？
                return p
            else:
                # WIT[p] = q - p + 1
                return q
        except Exception as e:
            print(f'{e}: p:{p}, q:{q}, j={j}, w={w}, q+w-1 = {q+w-1}')

    # 使用批处理优化块处理和验证
    def process_blocks_and_verify(blocks):
        results = []
        for start, end in blocks:
            candidates = list(range(start, end + 1))
            while len(candidates) > 1:
                next_candidates = []
                for i in range(0, len(candidates) - 1, 2):
                    winner = duel(candidates[i], candidates[i + 1])
                    next_candidates.append(winner)
                if len(candidates) % 2 == 1:
                    next_candidates.append(candidates[-1])
                candidates = next_candidates

            if candidates:
                pos = candidates[0]
                if all(T[pos + j] == P[j] for j in range(m)):
                    results.append(pos)
        return results

    # 动态分配线程
    max_workers = os.cpu_count() // 2
    chunk_size = len(blocks) // max_workers or 1  # 每个线程处理的块数量
    block_chunks = [blocks[i:i + chunk_size] for i in range(0, len(blocks), chunk_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        candidate_positions = sum(
            executor.map(process_blocks_and_verify, block_chunks), []
        )
    
    return candidate_positions

def get_period(WITNESS):
    # 根据WITNESS数组获取周期
    for i in range(2, len(WITNESS)):
        if WITNESS[i] == 0:
            return i - 1
    return 0

def match_pattern_period(T, P, WITNESS, period, n, m):
    k = m // period  # 完整周期数
    u = P[:period]   # 模式串的周期部分
    v = P[k * period:]  # 模式串的余部分

    # P的前缀P(1:2p-1)
    prefix_len = 2 * period - 1

    # 特殊优化处理：由于处理器较少，所以当周期较小的时候会产生非常多的块，导致通信开销过大，因此使用并行的KMP算法。
    if period < 5:
        return par_kmp_search(T, P)
    
    prefix_matches = match_pattern_non_period(T, P[:prefix_len], WITNESS, n, prefix_len)

    # 验证u^2v是否出现在每个候选位置
    def verify_u2v(start):
        """验证从start开始是否匹配u^2v"""
        if start + 2 * period + len(v) > n:
            return False
        # 直接验证u^2v部分
        return (T[start:start + period] == u and
                T[start + period:start + 2 * period] == u and
                T[start + 2 * period:start + 2 * period + len(v)] == v)

    # 并行验证
    with ThreadPoolExecutor(max_workers=10) as executor:
        match_flags = list(executor.map(verify_u2v, prefix_matches))
    
    # 直接记录匹配的位置
    match_positions = set()

    # 只对前缀匹配的位置进行进一步的验证
    for idx, start in enumerate(prefix_matches):
        if match_flags[idx]:  # 如果此位置匹配u^2v
            match_positions.add(start)

    # 对每个周期进行进一步的检查，直接在这里计算匹配
    def process_period(i):
        match_positions_local = deque()  # 使用 deque 来存储匹配位置，保证顺序
        consecutive_count = 0
        for l in range((n - i) // period, -1, -1):  # l >= 0, 逆序遍历
            idx = i + l * period
            if idx in match_positions:
                consecutive_count += 1
            else:
                consecutive_count = 0

            # 检查是否有至少k-1个连续的1
            if consecutive_count >= k - 1:
                match_positions_local.appendleft(i + l * period)  # 在头部插入元素

        return match_positions_local

    # 对每个周期进行检查
    match_positions_final = []
    with ThreadPoolExecutor(max_workers=min(8,period)) as executor:
        results = executor.map(process_period, range(period))

    # 汇总所有周期的匹配位置
    for result in results:
        match_positions_final.extend(result)

    return match_positions_final

def match_pattern(T, P):
    n = len(T)
    m = len(P)
    if n < m :
        return []

    # 经测试，这里2500是串行和并行性能的大概的临界点
    if m < 2500:
        witness = compute_witness_serial(P, m)
    else:
        witness = compute_witness_parallel_process(P, m, num_threads=4)
    
    period = get_period(witness)

    if period > 0:
        return match_pattern_period(T, P, witness, period, n, m) # 周期串
    elif period == 0:
        return match_pattern_non_period(T, P, witness, n, m) # 非周期串
    



if __name__ == "__main__":
    # 测试
    text = "abcabc"*100
    pattern = 'abcdbcaacdabcdbcaacdabcdbcaacd'

    m = len(pattern)

    time1 = time.time()
    # 并行
    wit = compute_witness_parallel_process(pattern, m, num_threads=10)
    print(f"Witness: {wit}")
    # 串行
    wit = compute_witness_serial(pattern, m)

    time2 = time.time()
    print(f"Witness: {wit}")
    print(f"Witness Time: {time2 - time1:.2f}s")

    time1 = time.time()
    matches = match_pattern(text, pattern)
    time2 = time.time()
    print(f"#matched: {len(matches)}\nMatching positions: {matches if len(matches) < 100 else 'too long hidden'}, Time: {time2 - time1:.2f}s")
    # print('text size: ',sys.getsizeof(text))

    time1 = time.time()
    matches2 = par_kmp_search(text, pattern)
    time2 = time.time()
    print(f"#matched: {len(matches2)}\nMatching positions: {matches2 if len(matches2) < 100 else 'too long hidden'}, Time: {time2 - time1:.2f}s")
    print(f"is_same: {matches==matches2}")
