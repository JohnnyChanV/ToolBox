import threading
import time

import pandas as pd
from tqdm import tqdm
from googletranslatepy import Translator
import json
import concurrent.futures as cf
from clash_proxy_controller import clash_proxy_controller


class ListProcessor():
    def __init__(self,data:list, batch_size = 64):
        self.data = data
        self.batch_size = batch_size
        self.batched_data = self.split_batch(batch_size)
        self.failed_instances = []

    def split_batch(self,size):
        self.batch_size = size
        data = self.data
        L = len(data)
        batch = []
        indices = list(range(0,L,size))+[L]
        for i in range(len(indices)-1):
            batch.append(data[indices[i]:indices[i+1]])

        tmp = []
        for b in batch:
            tmp+=b
        assert len(tmp) == len(data), f"batched_num:{len(tmp)}, original num:{len(data)}"
        self.batched_data = batch

        return batch

    def iter(self,instance):
        translator = Translator()
        is_success = True

        headline = instance['标题']
        content = instance['内容']

        translated_headline = translator.translate(headline)
        translated_content = translator.translate(content)

        instance['headline_traslated'] = translated_headline
        instance['content_traslated'] = translated_content
        if translated_headline==False or translated_content==False:
            is_success = False

        return instance, is_success

    def iterInMultiThread(self,need_proxy=False):
        # Split data into batches
        batches = self.split_batch(size=self.batch_size)
        num_threads = self.batch_size
        bar = tqdm(total=len(self.data))
        resultsInBatch = []

        if need_proxy:
            clash = clash_proxy_controller()

        with cf.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for batch in batches:
                if need_proxy:
                    clash.change_proxy()
                futures = [executor.submit(self.iter, instance) for instance in batch]
                for future in cf.as_completed(futures):
                    instance, is_success = future.result()
                    if not is_success:
                        self.failed_instances.append(instance)
                    else:
                        resultsInBatch.append(instance)
                    bar.update(1)
        return resultsInBatch

    def get_failed_example_num(self):
        return len(self.failed_instances)

    def get_failed_examples(self):
        return (self.failed_instances)

    def retry_failed_examples(self,need_proxy=False,retry_depth=5):
        if len(self.failed_instances)==0:
            return []

        successful_retried_instances = []

        current_depth = 0
        while len(self.failed_instances)!=0 and current_depth < retry_depth:
            this_processor = ListProcessor(self.failed_instances)
            successful_retried_instances += this_processor.iterInMultiThread(need_proxy)
            self.failed_instances = this_processor.get_failed_examples()
            current_depth+=1
        return successful_retried_instances,self.failed_instances
