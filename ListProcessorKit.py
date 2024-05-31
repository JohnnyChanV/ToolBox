from tqdm import tqdm
import concurrent.futures as cf

class ListProcessor():
    def __init__(self,iter_func, data:list, batch_size = 64):
        self.data = data
        self.batch_size = batch_size
        self.batched_data = self.split_batch(batch_size)
        self.failed_instances = []
        self.iter_func = iter_func

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
        try:
            ret = self.iter_func(instance)
            is_success = True
        except:
            ret = None
            is_success = False
        
        
        return ret, is_success

    def iterInMultiThread(self):
        # Split data into batches
        batches = self.split_batch(size=self.batch_size)
        num_threads = self.batch_size
        bar = tqdm(total=len(self.data))
        resultsInBatch = []


        with cf.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for batch in batches:
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

    def retry_failed_examples(self,retry_depth=5):
        if len(self.failed_instances)==0:
            return []

        successful_retried_instances = []

        current_depth = 0
        while len(self.failed_instances)!=0 and current_depth < retry_depth:
            this_processor = ListProcessor(self.iter_func,self.failed_instances)
            successful_retried_instances += this_processor.iterInMultiThread()
            self.failed_instances = this_processor.get_failed_examples()
            current_depth+=1
        return successful_retried_instances,self.failed_instances
