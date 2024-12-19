import concurrent.futures
import os

def kmp_search(text, pattern):
    m, n = len(pattern), len(text)
    lps = [0] * m 
    j = 0
    results = []
    
    def compute_lps():
        length = 0
        i = 1
        while i < m:
            if pattern[i] == pattern[length]:
                length += 1
                lps[i] = length
                i += 1
            else:
                if length != 0:
                    length = lps[length - 1]
                else:
                    lps[i] = 0
                    i += 1

    compute_lps()

    i = 0  # text index
    while i < n:
        if pattern[j] == text[i]:
            i += 1
            j += 1
        
        if j == m:
            results.append(i - j)
            j = lps[j - 1]
        elif i < n and pattern[j] != text[i]:
            if j != 0:
                j = lps[j - 1]
            else:
                i += 1
    return results

def par_kmp_search(text, pattern, num_threads=os.cpu_count() // 2):
    text_len = len(text)
    pattern_len = len(pattern)
    # 每个块的大小，至少是模式串长度
    block_size = max(text_len // num_threads, pattern_len)
    overlap_size = pattern_len - 1  # 重叠部分的大小
    futures = {}

    # 创建线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # 将文本分块并分配任务
        for i in range(num_threads):
            start = i * block_size
            # 最后一块需要处理剩余的部分
            end = (i + 1) * block_size if i < num_threads - 1 else text_len
            block_end = end if i == num_threads - 1 else end + overlap_size
            
            futures[executor.submit(kmp_search, text[start:block_end], pattern)] = start

        # 收集结果
        results = {}
        for future in concurrent.futures.as_completed(futures):
            block_start = futures[future]
            results[block_start] = future.result()

    # 合并每个块的匹配结果
    matches = []
    for block_start, result in results.items():
        for match in result:
            matches.append(block_start + match)

    return matches

if __name__ == '__main__':
    text = "ababcababcababcabc"*100
    pattern = "ababcaba"

    matches = par_kmp_search(text, pattern, num_threads=8)
    print(f'Matches found at indices: {matches[:10]}')

    matches2 = kmp_search(text, pattern)
    print(f'Matches found at indices: {matches2[:10]}')

    print(f"Matched: {sorted(matches)==sorted(matches2)}")
