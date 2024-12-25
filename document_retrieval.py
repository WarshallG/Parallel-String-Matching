from kmp import kmp_search
from parallel_matching import match_pattern
import time
import os
import concurrent.futures
from tqdm import tqdm
from kmp import par_kmp_search

def match_helper(text, pattern, match_type, num_processes = 10):
    if match_type == 'match_pattern':  # 书上的并行字符串匹配算法
        func = match_pattern
    elif match_type == 'kmp_search':   # 串行KMP算法
        func = kmp_search
    elif match_type == 'par_kmp_search':  # 并行KMP算法
        func = par_kmp_search
    else:
        print("Invalid match type")
        return []
        
    m = len(pattern)
    n = len(text)
    block_size = n // num_processes
    results = []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = dict()
        for i in range(num_processes):
            start = i * block_size
            end = (i + 1) * block_size if i < num_processes - 1 else n
            block_end = end if i == num_processes - 1 else end + m - 1

            futures[executor.submit(func, text[start:block_end], pattern)] = start

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            block_start = futures[future]
            result = [pos+block_start for pos in result]
            results.extend(result)

    return sorted(results)

if __name__ == '__main__':
    # 定义文件路径
    target_path = '../data/document_retrieval/document.txt'
    pattern_path = '../data/document_retrieval/target.txt'


    print("文件读取中...")
    # 打开文件并读取内容
    with open(target_path, 'r', encoding='utf-8') as file:
        target = file.read()

    with open(pattern_path, 'r', encoding='utf-8') as file:
        pattern_list = file.read()

    pattern_list = pattern_list.split('\n')
    print("文件读取完毕.")


    print("并行字符串匹配算法检测...")
    start_time = time.time()
    match_list = []
    for idx, pattern in enumerate(pattern_list):
        print(f"正在匹配第 {idx + 1} 个模式: {pattern}")
        # matches = kmp_search(target, pattern)
        time1 = time.time()
        matches = match_helper(target, pattern, "match_pattern")
        matches.sort()
        match_list.append(matches)
        print(f'第{idx}个模式匹配结束. 用时{time.time() - time1:.2f}s.')
        print(f'匹配点为：{matches}\n')    

    print(f"匹配结束，用时{time.time() - start_time:.2f}s")

    # # 保存输出
    # output_file = '../result_document.txt'

    # with open(output_file, 'w', encoding='utf-8') as file:
    #     for lst in match_list:
    #         line = f"{len(lst)} " + " ".join(map(str, lst))
    #         file.write(line + "\n")

    # print(f"匹配点已保存在{output_file}中")


    # print("串行KMP算法检测...")
    # start_time = time.time()
    # for idx, pattern in enumerate(pattern_list):
    #     print(f"正在匹配第 {idx + 1} 个模式: {pattern}")
    #     time1 = time.time()
    #     matches = kmp_search(target, pattern)
    #     print(f'第{idx}个模式匹配结束. 用时{time.time() - time1:.2f}s.') 

    # print(f"串行KMP测试结束，用时: {time.time() - start_time:.2f}s.\n")


    # print("并行KMP算法检测...")
    # start_time = time.time()
    # for idx, pattern in enumerate(pattern_list):
    #     print(f"正在匹配第 {idx + 1} 个模式: {pattern}")
    #     time1 = time.time()
    #     # matches = match_helper(target, pattern, "par_kmp_search")
    #     matches = par_kmp_search(target, pattern)
    #     print(f'第{idx}个模式匹配结束. 用时{time.time() - time1:.2f}s.') 

    # print(f"并行KMP测试结束，用时: {time.time() - start_time:.2f}s.")
