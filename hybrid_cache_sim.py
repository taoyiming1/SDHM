import collections
import re
import os
from random import randint
import time
import sys

SAMPLE_INPUT_DIR = "C:\\Users\\Administrator\\Desktop\\trace\\trace_5"
INPUT_DIR = 'C:\\Users\\Administrator\\Desktop\\trace\\trace_6'
DIR = 'C:\\Users\\Administrator\\Desktop\\trace\\trace_'
RATIO = 4
PARAM = 5
UNIT_CACHE_SIZE = 8192
class File():
    def __init__(self, fid=0, size = 0):
        self.fid = fid
        self.size = size
        self.request_count = 0
        self.read_count = 0
        self.write_count = 0
        self.last_visit = 0
        self.near = sys.maxint
        self.next = None
        self.pre = None
        self.right = None
        self.left = None
        
class hybrid_cache():
    def __init__(self, size, file_map, hybrid_type ):
        self.ssd_cache = ssd(size/PARAM*(PARAM-1), file_map)
        self.nvram_cache = None
        if hybrid_type == 'lru':
            self.nvram_cache = nvram_lru(size/PARAM)
        elif hybrid_type == 'lfu':
            self.nvram_cache = nvram_lfu(size/PARAM)
        elif hybrid_type == 'random':
            self.nvram_cache = nvram_random(size/PARAM)
        self.read_count = 0
        self.read_hit = 0
        self.read_miss = 0
        self.write_count = 0
        self.write_hit = 0
        self.write_miss = 0
        
    def read(self, read_file, read_size):
        self.read_count += 1
        if read_file.fid in self.ssd_cache.file_map:
            self.ssd_cache.read(read_file, read_size)
            self.read_hit += 1
            return True
        elif read_file.fid in self.nvram_cache.file_map:
            self.nvram_cache.read(read_file, read_size)
            self.read_hit +=1
            return True
        else:
            self.nvram_cache.write(read_file, read_file.size)
            self.read_miss +=1
            return False
            return 
        return

    def write(self,write_file, write_size):
        self.write_count += 1
        if write_file.fid in self.ssd_cache.file_map:
            self.ssd_cache.write(write_file,write_size)
            self.write_hit += 1
        elif write_file.fid in self.nvram_cache.file_map:
            self.nvram_cache.write(write_file, write_size)
            self.write_hit +=1
        else:
            self.nvram_cache.write(write_file,write_file.size)
            self.write_miss +=1
        return
            
    
class ssd():
    def __init__(self, size, file_map):
        self.capacity_available = size
        self.size = size
        self.file_map = file_map
        self.request_count = 0
        self.read_count = 0
        self.read_hit = 0
        self.read_miss = 0
        self.write_count = 0
        self.write_hit = 0
        self.write_miss = 0
        self.total_write_size = 0
        
    def write(self,write_file, write_size):
        self.write_count += 1
        if write_file.fid in self.file_map:
            self.write_hit += 1
            self.total_write_size += write_size
        else:
            self.write_miss += 1
        return

    def read(self,read_file,read_size):
        self.read_count += 1
        if read_file.fid in self.file_map:
            self.read_hit += 1
            return True
        else:
            self.read_miss += 1
            return False
        return
    
class nvram_lru():
    def __init__(self, size):
        self.size = size
        self.size_used = 0
        self.size_available = size
        self.file_map = dict()
        
        self.q_head = File()
        self.q_tail = File()
        self.q_head.next = self.q_tail
        self.q_tail.pre = self.q_head
        self.q_count = 0

        self.read_count = 0
        self.read_hit = 0
        self.read_miss = 0
        self.write_count = 0
        self.write_hit = 0
        self.write_miss = 0
        self.total_write_size = 0                 

    def write(self,write_file, write_size):
        if write_file.fid in self.file_map:
            self.write_hit += 1
            self.update_q(write_file)
            self.total_write_size += write_size
        elif self.size_available >= write_file.size:
            self.write_miss += 1
            self.append_q(write_file)
            self.total_write_size += write_file.size
        elif self.size >= write_file.size:
            self.write_miss += 1
            while True:
                pop_file = self.pop_q()
                #self.size_available += pop_file.size
                if self.size_available >= write_file.size:
                    self.append_q(write_file)
                    break
            self.total_write_size += write_file.size
        else:
            self.write_miss += 1
            return
        
        self.write_count += 1
        return
            
    def read(self,read_file, read_size):
        result = False
        if read_file.fid in self.file_map:
            self.read_hit += 1
            self.update_q(read_file)
            result = True
        elif self.size_available >= read_file.size:
            self.read_miss += 1
            #self.append_q(read_file)
            self.write(read_file, read_file.size)
            self.total_write_size += read_file.size
            result = False
        elif self.size >= read_file.size:
            self.read_miss += 1
            self.write(read_file, read_file.size)
            result = False
            
        else:
            self.read_miss += 1
        self.read_count += 1
        return result
            
    def update_q(self, update_file):
        if update_file.fid in self.file_map:
            cur_file = self.file_map[update_file.fid]
            pre_file = cur_file.pre
            next_file = cur_file.next
            pre_file.next = next_file
            next_file.pre = pre_file
            last_file = self.q_tail.pre
            
            cur_file.pre = last_file
            cur_file.next = self.q_tail
            last_file.next = cur_file
            self.q_tail.pre = cur_file

        else:
            print 'file not in map, can not update!'
            return False

    def pop_q(self):
        if self.q_count > 0:
            cur_file = self.q_head.next
            next_file = cur_file.next
            self.q_head.next = next_file
            next_file.pre = self.q_head
            cur_file.pre = None
            cur_file.next = None
            self.q_count -= 1
            del self.file_map[cur_file.fid]
            self.size_available += cur_file.size
            self.size_used -= cur_file.size
            return cur_file
        else:
            print "No file to pop!"
            return None
            
    def append_q(self, append_file):
        if append_file.fid in self.file_map:
            print 'file alread existed!'
            return None
        if self.size_available >= append_file.size:
            self.file_map[append_file.fid] = append_file
            last_file = self.q_tail.pre
            append_file.pre = last_file
            append_file.next = self.q_tail
            last_file.next = append_file
            self.q_tail.pre = append_file
            self.q_count += 1
            self.size_available -= append_file.size
            self.size_used += append_file.size
        else:
            print 'No space to append file'
            return None
        
    def printq(self):
        temp = self.q_head.next
        for i in range(0, self.q_count):
            print temp.fid, temp.size
            temp = temp.next
        
            
    def get_q_length(self):
        return self.q_count
    
class nvram_random():
    def __init__(self, size):
        self.size = size
        self.size_used = 0
        self.size_available = size
        self.file_map = dict()

        self.q_count = 0
        self.read_count = 0
        self.read_hit = 0
        self.read_miss = 0
        self.write_count = 0
        self.write_hit = 0
        self.write_miss = 0
        self.total_write_size = 0                 

    def write(self,write_file, write_size):
        if write_file.fid in self.file_map:
            self.write_hit += 1
            self.total_write_size += write_size
        elif self.size_available >= write_file.size:
            self.write_miss += 1
            self.append_q(write_file)
            self.total_write_size += write_file.size
        elif self.size >= write_file.size:
            self.write_miss += 1
            while True:
                pop_file = self.pop_q()
                #self.size_available += pop_file.size
                if self.size_available >= write_file.size:
                    self.append_q(write_file)
                    break
            self.total_write_size += write_file.size
        else:
            self.write_miss += 1
            return
        
        self.write_count += 1
        return
            
    def read(self,read_file, read_size):
        result = False
        if read_file.fid in self.file_map:
            self.read_hit += 1
            result = True
        elif self.size_available >= read_file.size:
            self.read_miss += 1
            self.write(read_file, read_file.size)
            self.total_write_size += read_file.size
            result = False
        elif self.size >= read_file.size:
            self.read_miss += 1
            self.write(read_file, read_file.size)
            result = False
            
        else:
            self.read_miss += 1
        self.read_count += 1
        return result
            

    def pop_q(self):
        if self.q_count > 0:
            keys = self.file_map.keys()
            key = keys[randint(0,self.q_count-1)]
            cur_file = self.file_map[key]
            self.size_available += cur_file.size
            self.size_used -= cur_file.size
            self.q_count -= 1
            del self.file_map[cur_file.fid]
            
            if self.size_used < 0:
                print 'self.size_used',self.size_used
                print 'self.size_available',self.size_available
                print 'self.size',self.size
                print 'cur_file.size',cur_file.size
                self.size_available = self.size
                self.size_used = 0
            return cur_file
        else:
            print "No file to pop!"
            return None
            
    def append_q(self, append_file):
        if append_file.fid in self.file_map:
            print 'file alread existed!'
            return None
        if self.size_available >= append_file.size:
            self.file_map[append_file.fid] = append_file
            self.q_count += 1
            self.size_available -= append_file.size
            self.size_used += append_file.size
        else:
            print 'No space to append file'
            return None
        
            
    def get_q_length(self):
        return self.q_count
    
class nvram_lfu():
    def __init__(self, size):
        self.size = size
        self.size_used = 0
        self.size_available = size
        self.file_map = dict()
        self.frequency_map = dict()
        self.q_count = 0
        
        self.head = File()
        self.tail = File()
        self.head.right = self.tail
        self.tail.left = self.head
        
        self.read_count = 0
        self.read_hit = 0
        self.read_miss = 0
        self.write_count = 0
        self.write_hit = 0
        self.write_miss = 0
        self.total_write_size = 0                 

    def write(self,write_file, write_size):
        if write_file.fid in self.file_map:
            self.write_hit += 1
            self.update_q(write_file)
            self.total_write_size += write_size
        elif self.size_available >= write_file.size:
            self.write_miss += 1
            self.append_q(write_file)
            self.total_write_size += write_file.size
        elif self.size >= write_file.size:
            self.write_miss += 1
            while True:
                pop_file = self.pop_q()
                #self.size_available += pop_file.size
                if self.size_available >= write_file.size:
                    self.append_q(write_file)
                    break
            self.total_write_size += write_file.size
        else:
            self.write_miss += 1
            return
        
        self.write_count += 1
        return
            
    def read(self,read_file, read_size):
        result = False
        if read_file.fid in self.file_map:
            self.read_hit += 1
            self.update_q(read_file)
            result = True
        elif self.size_available >= read_file.size:
            self.read_miss += 1
            #self.append_q(read_file)
            self.write(read_file, read_file.size)
            self.total_write_size += read_file.size
            result = False
        elif self.size >= read_file.size:
            self.read_miss += 1
            self.write(read_file, read_file.size)
            result = False
            
        else:
            self.read_miss += 1
        self.read_count += 1
        return result
            
    def update_q(self, update_file):
        if update_file.fid in self.file_map:
            cur_file = self.file_map[update_file.fid]
            
            pre_file = cur_file.pre
            next_file = cur_file.next
            pre_file.next = next_file
            next_file.pre = pre_file
            
            if self.frequency_map[cur_file.request_count][2] > 0:
                self.frequency_map[cur_file.request_count][2] -= 1
            else:
                print 'negative request_count!'
                exit()
            cur_file.request_count += 1
            if not cur_file.request_count in self.frequency_map:
                left = self.frequency_map[cur_file.request_count-1][0]
                right = self.frequency_map[cur_file.request_count-1][0].right
                head = File()
                tail = File()
                head.next = tail
                tail.pre = head
                self.frequency_map[cur_file.request_count] = [head, tail, 0]
                
                left.right = head
                head.left = left
                right.left = head
                head.right = right
            
            tail = self.frequency_map[cur_file.request_count][1]
            last_file = tail.pre
            cur_file.pre = last_file
            cur_file.next = tail
            last_file.next = cur_file
            tail.pre = cur_file
            self.frequency_map[cur_file.request_count][2] += 1
            if  self.frequency_map[cur_file.request_count-1][2] == 0:
                del_head = self.frequency_map[cur_file.request_count-1][0]
                left = del_head.left
                right = del_head.right
                right.left = left
                left.right = right
                del_head.left = None
                del_head.right = None
                del self.frequency_map[cur_file.request_count-1]
                
        else:
            print 'file not in map, can not update!'
            return False

    def pop_q(self):
        if self.q_count > 0:
            head = self.head.right
            cur_file = head.next
            next_file = cur_file.next
            head.next = next_file
            next_file.pre = head
            cur_file.pre = None
            cur_file.next = None
            self.q_count -= 1
            del self.file_map[cur_file.fid]
            self.size_available += cur_file.size
            self.size_used -= cur_file.size
            self.frequency_map[cur_file.request_count][2] -= 1
            if self.frequency_map[cur_file.request_count][2] == 0:
                left = self.frequency_map[cur_file.request_count][0].left
                right = self.frequency_map[cur_file.request_count][0].right
                self.frequency_map[cur_file.request_count][0].left = None
                self.frequency_map[cur_file.request_count][0].right = None
                left.right = right
                right.left = left
                del self.frequency_map[cur_file.request_count]
                
            return cur_file
        else:
            print "No file to pop!"
            return None
            
    def append_q(self, append_file):
        if append_file.fid in self.file_map:
            print 'file alread existed!'
            return None
        if self.size_available >= append_file.size:
            self.file_map[append_file.fid] = append_file
            append_file.request_count = 1
            if not 1 in self.frequency_map:
                head = File()
                tail = File()
                head.next = tail
                tail.pre = head
                self.frequency_map[1] = [head, tail, 0]
                next_head = self.head.right
                
                head.right = next_head
                head.left = self.head
                self.head.right = head
                next_head.left = head
                
            
            tail = self.frequency_map[1][1]
            last_file = tail.pre
            
            append_file.pre = last_file
            append_file.next = tail
            last_file.next = append_file
            tail.pre = append_file
            self.frequency_map[1][2] += 1
            
            self.q_count += 1
            self.size_available -= append_file.size
            self.size_used += append_file.size
            return
        else:
            print 'No space to append file'
            return None
        
            
    def get_q_length(self):
        return self.q_count
    
def get_map(input_dir):
    sample_input_file = open(input_dir)
    return_list = sample_input_file.readlines()
    file_map = dict()
    for entry in return_list:
        try:
            matchObj = re.match( r'.* (.*) (.*) FID: (.*) OFF: (\d*) SIZE: (\d*) HOST: (.*)\n', entry)
            category = matchObj.group(1)
            rw = matchObj.group(2)
            fid = matchObj.group(3)
            size = int(matchObj.group(5))
            if size > 0:
                if category == 'Close':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    if temp_file.size < size:
##                        temp_file.size = size
                    temp_file.request_count += 1
##                    if rw == 'READ':
##                        temp_file.read_count += 1
##                    elif rw == 'WRITE':
##                        temp_file.write_count += 1
##                    elif rw == 'RW':
##                        temp_file.read_count += 1
##                        temp_file.write_count += 1
##                    else:
##                        print 'err',rw
##                        exit()
                elif category == 'Open':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    if temp_file.size < size:
##                        temp_file.size = size
                    temp_file.request_count += 1
##                    if rw == 'READ':
##                        temp_file.read_count += 1
##                    elif rw == 'WRITE':
##                        temp_file.write_count += 1
##                    elif rw == 'RW':
##                        temp_file.read_count += 1
##                        temp_file.write_count += 1
##                    else:
##                        print 'err',rw
##                        exit()
                elif category == 'Delete':
                    if fid in file_map:
                        del file_map[fid]
                    temp_file.request_count += 1
                elif category in ['Block', 'Dir']:
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
                    temp_file.request_count += 1
                    if rw == 'READ':
                        temp_file.read_count += 1
                    elif rw == 'WRITE':
                        temp_file.write_count += 1
                    elif rw == 'RW':
                        temp_file.read_count += 1
                        temp_file.write_count += 1
                    else:
                        print 'err',rw
                        exit()
        except:
            print 'except',entry
            
    return file_map




def calculate_hit_ratio_prob(input_dir, cache_size, popular_map):
    sample_input_file = open(input_dir)
    return_list = sample_input_file.readlines()
    file_map = dict()
    cache = ssd(cache_size, popular_map)
    line = 0
    
    for entry in return_list:
        line += 1
        try:
            matchObj = re.match( r'.* (.*) (.*) FID: (.*) OFF: (\d*) SIZE: (\d*) HOST: (.*)\n', entry)
            category = matchObj.group(1)
            rw = matchObj.group(2)
            fid = matchObj.group(3)
            size = int(matchObj.group(5))
            
            if size > 0:
                if category == 'Close':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    if temp_file.size < size:
##                        temp_file.size = size
##                    temp_file.request_count += 1
                elif category == 'Open':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    if temp_file.size < size:
##                        temp_file.size = size
##                    temp_file.request_count += 1

                elif category == 'Delete':
                    if fid in file_map:
                        del file_map[fid]
##                    temp_file.request_count += 1
                elif category in ['Block', 'Dir']:
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    temp_file.request_count += 1
                    if rw == 'READ':
                        cache.read(temp_file,size)
                        temp_file.read_count += 1
                    elif rw == 'WRITE':
                        cache.write(temp_file,size)
                        temp_file.write_count += 1
                    elif rw == 'RW':
                        if cache.read(temp_file,size):
                            cache.write(temp_file,size)
                        temp_file.read_count += 1
                        temp_file.write_count += 1
                    else:
                        print 'err',rw
                        exit()
        except:
            print 'except!'
            pass
    print 'cache.read_count',cache.read_count
    print 'cache.read_hit',cache.read_hit
    print 'cache.read_miss',cache.read_miss
    print 'cache.write_count',cache.write_count
    print 'cache.write_hit',cache.write_hit
    print 'cache.write_miss',cache.write_miss
    print 'cache.total_write_size',cache.total_write_size
    return file_map
            
def calculate_hit_ratio_ssd(input_dir ,cache_size, cache_type):
    sample_input_file = open(input_dir)
    return_list = sample_input_file.readlines()
    file_map = dict()
    cache = None
    
    if cache_type == 'random':
        cache = nvram_random(cache_size)
    elif cache_type == 'lru':
        cache = nvram_lru(cache_size)
    elif cache_type == 'lfu':
        cache = nvram_lfu(cache_size)
    else:
        print 'cache type error'
        exit()
        
    request_line_count = 1
    for entry in return_list:
        try:
            matchObj = re.match( r'.* (.*) (.*) FID: (.*) OFF: (\d*) SIZE: (\d*) HOST: (.*)\n', entry)
            category = matchObj.group(1)
            rw = matchObj.group(2)
            fid = matchObj.group(3)
            size = int(matchObj.group(5))
            request_line_count += 1
            if size > 0:
                if category == 'Close':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    if temp_file.size < size:
##                        temp_file.size = size
                    #temp_file.request_count += 1
##                    if rw == 'READ':
##                        cache.read(temp_file,size)
##                        temp_file.read_count += 1
##                    elif rw == 'WRITE':
##                        cache.write(temp_file,size)
##                        temp_file.write_count += 1
##                    elif rw == 'RW':
##                        if cache.read(temp_file,size):
##                            cache.write(temp_file,size)
##                        temp_file.read_count += 1
##                        temp_file.write_count += 1
##                    else:
##                        print 'err',rw
##                        exit()
                elif category == 'Open':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]

##                    if temp_file.size < size:
##                        temp_file.size = size
                    #temp_file.request_count += 1
##                    if rw == 'READ':
##                        cache.read(temp_file,size)
##                        temp_file.read_count += 1
##                    elif rw == 'WRITE':
##                        cache.write(temp_file,size)
##                        temp_file.write_count += 1
##                    elif rw == 'RW':
##                        if cache.read(temp_file,size):
##                            cache.write(temp_file,size)
##                        temp_file.read_count += 1
##                        temp_file.write_count += 1
##                    else:
##                        print 'err',rw
##                        exit()
                elif category == 'Delete':
                    if fid in file_map:
                        del file_map[fid]
                    #temp_file.request_count += 1
                elif category in ['Block', 'Dir']:
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
                    #temp_file.request_count += 1
                    if rw == 'READ':
                        cache.read(temp_file,size)
                        temp_file.read_count += 1
                    elif rw == 'WRITE':
                        cache.write(temp_file,size)
                        temp_file.write_count += 1
                    elif rw == 'RW':
                        if cache.read(temp_file,size):
                            cache.write(temp_file,size)
                        temp_file.read_count += 1
                        temp_file.write_count += 1
                    else:
                        print 'err',rw
                        exit()
                
        except:
            print 'except!'
            pass 
    print 'cache.read_count',cache.read_count
    print 'cache.read_hit',cache.read_hit
    print 'cache.read_miss',cache.read_miss
    print 'cache.write_count',cache.write_count
    print 'cache.write_hit',cache.write_hit
    print 'cache.write_miss',cache.write_miss
    print 'cache.size_available',cache.size_available
    print 'cache.size_used',cache.size_used
    print 'cache.size',cache.size
    print 'total_write_size',cache.total_write_size
    return file_map

def calculate_hit_ratio_hybrid(input_dir ,cache_size, popular_map,cache_type):
    sample_input_file = open(input_dir)
    return_list = sample_input_file.readlines()
    file_map = dict()
    cache = hybrid_cache(cache_size, popular_map, cache_type)
    
    for entry in return_list:
        try:
            matchObj = re.match( r'.* (.*) (.*) FID: (.*) OFF: (\d*) SIZE: (\d*) HOST: (.*)\n', entry)
            category = matchObj.group(1)
            rw = matchObj.group(2)
            fid = matchObj.group(3)
            size = int(matchObj.group(5))
        
        
            if size > 0:
                if category == 'Close':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    if temp_file.size < size:
##                        temp_file.size = size
                   
                elif category == 'Open':
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
##                    if temp_file.size < size:
##                        temp_file.size = size
                    #temp_file.request_count += 1
##                    if rw == 'READ':
##                        cache.read(temp_file,size)
##                        temp_file.read_count += 1
##                    elif rw == 'WRITE':
##                        cache.write(temp_file,size)
##                        temp_file.write_count += 1
##                    elif rw == 'RW':
##                        if cache.read(temp_file,size):
##                            cache.write(temp_file,size)
##                        temp_file.read_count += 1
##                        temp_file.write_count += 1
##                    else:
##                        print 'err',rw
##                        exit()
                elif category == 'Delete':
                    if fid in file_map:
                        del file_map[fid]
                    #temp_file.request_count += 1
                elif category in ['Block', 'Dir']:
                    if not fid in file_map:
                        file_map[fid] = File(fid, size)
                    temp_file = file_map[fid]
                    #temp_file.request_count += 1
                    if rw == 'READ':
                        cache.read(temp_file,size)
                        temp_file.read_count += 1
                    elif rw == 'WRITE':
                        cache.write(temp_file,size)
                        temp_file.write_count += 1
                    elif rw == 'RW':
                        if cache.read(temp_file,size):
                            cache.write(temp_file,size)
                        temp_file.read_count += 1
                        temp_file.write_count += 1
                    else:
                        print 'err',rw
                        exit()
        except:
            print 'except'

    print 'cache.ssd_cache.size',cache.ssd_cache.size
    print 'cache.nvram_cache.size',cache.nvram_cache.size
    print 'cache.read_count',cache.read_count
    print 'cache.read_hit',cache.read_hit
    print 'cache.read_miss',cache.read_miss
    print 'cache.write_count',cache.write_count
    print 'cache.write_hit',cache.write_hit
    print 'cache.write_miss',cache.write_miss
    print 'cache.nvram_cache.size_available',cache.nvram_cache.size_available
    print 'cache.nvram_cache.size_used',cache.nvram_cache.size_used
    print 'cache.nvram_cache.size',cache.nvram_cache.size
    print 'cache.nvram_cache.total_write_size',cache.ssd_cache.total_write_size
    return file_map



def sim(sample_input_dir, input_dir):

    print '****random*******' 
    calculate_hit_ratio_ssd(input_dir ,UNIT_CACHE_SIZE*RATIO,'random')
    print '****lru*******' 
    calculate_hit_ratio_ssd(input_dir ,UNIT_CACHE_SIZE*RATIO,'lru')
    print '****lfu*******' 
    calculate_hit_ratio_ssd(input_dir ,UNIT_CACHE_SIZE*RATIO,'lfu')
   
    return 

def sim2(sample_input_dir, input_dir, previous_popular_map,previous_popular_half_map):
    file_map = get_map(sample_input_dir)
    total_size = 0
    total_request_count = 0
    sort_list = []
    file_count = 0
    for fid in file_map:
        file_count += 1
        total_size += file_map[fid].size
        total_request_count += file_map[fid].request_count
        sort_list.append(file_map[fid])
    print 'len of files', file_count, len(file_map)
    print 'total_size', total_size
    print 'total_request_count', total_request_count
    
    cur_map = get_map(input_dir)
    second_day_total_size = 0
    second_day_total_request_count = 0
    second_day_sort_list = []
    second_day_file_count = 0
    for fid in cur_map:
        second_day_file_count += 1
        second_day_total_size += cur_map[fid].size
        second_day_total_request_count += cur_map[fid].request_count
        second_day_sort_list.append(cur_map[fid])
    print 'second_day_len of files', second_day_file_count
    print 'second_day_total_size', second_day_total_size
    print 'second_day_total_request_count', second_day_total_request_count
    
    average_size = float(total_size)/file_count
    print average_size
    average_request_count = float(total_request_count)/file_count
    print average_request_count

    request_sort_list= sorted(sort_list, key=lambda x: float(x.size)/x.request_count, reverse=False)
    popular_map = dict()
    popular_half_map = None
    
    popular_request = 0
    popular_request_size = 0
    popular_request_size_limit = 0

##    for ele in request_sort_list:
##        if ele.size == 0:
##            pass
##            
##        else:
##            weight = float(ele.request_count)/ele.size*float(average_size)/average_request_count
##            if weight > 1.0:
##                popular_request_size_limit += ele.size
                
    for ele in request_sort_list:
        if ele.size == 0:
            print 'zero size file here'
        else:
##            weight = float(ele.request_count)/ele.size*float(average_size)/average_request_count
##            if weight > 1.0:
            if (popular_request_size + ele.size) >= (UNIT_CACHE_SIZE*RATIO/PARAM*(PARAM-1)) and not popular_half_map:
                popular_half_map = dict(popular_map)
                print 'half popular size',popular_request_size
            if (popular_request_size + ele.size) >= UNIT_CACHE_SIZE*RATIO:
                print 'break'
                break
            popular_request += ele.request_count
            popular_map[ele.fid] = ele
            popular_request_size += ele.size
            
    print 'popular file len', len(popular_map)
    print 'popular file len half', len(popular_half_map)
    print 'popular_request ',popular_request
    print 'popular_request_size ',popular_request_size

    temp_total_request = 0
    for ele in cur_map:
        temp_total_request += cur_map[ele].request_count
    print 'temp_total_request ',temp_total_request
    print '****prob*******'   
    calculate_hit_ratio_prob(input_dir,UNIT_CACHE_SIZE*RATIO,popular_map)
##    print '****random*******' 
##    calculate_hit_ratio_ssd(input_dir ,UNIT_CACHE_SIZE*RATIO,'random')
##    print '****lru*******' 
##    calculate_hit_ratio_ssd(input_dir ,UNIT_CACHE_SIZE*RATIO,'lru')
##    print '****lfu*******' 
##    calculate_hit_ratio_ssd(input_dir ,UNIT_CACHE_SIZE*RATIO,'lfu')
   
    print '****hybrid_random*******'
    calculate_hit_ratio_hybrid(input_dir ,UNIT_CACHE_SIZE*RATIO, popular_half_map,'random')
    print '****hybrid_LRU*******'
    calculate_hit_ratio_hybrid(input_dir ,UNIT_CACHE_SIZE*RATIO, popular_half_map,'lru')
    print '****hybrid_lfu*******'
    calculate_hit_ratio_hybrid(input_dir ,UNIT_CACHE_SIZE*RATIO, popular_half_map,'lfu')
    return [popular_map,popular_half_map]

def main():
    previous_popular_map = None
    previous_popular_half_map = None
    param_list = [4,16,64]
    ratio_list = [64,256,1024]
    for ratio in ratio_list:
        global RATIO
        RATIO = ratio
        print '########ratio########',ratio
        for i in range(0,6):
                print '########i########',i
                sample_input_dir = DIR + str(i)
                input_dir = DIR + str(i+1)
                sim(sample_input_dir,input_dir)

    for ratio in ratio_list:
        global RATIO
        RATIO = ratio
        print '########ratio########',ratio
        for param in param_list:
            global PARAM
            PARAM = param
            print '########param########',param
            for i in range(0,6):
                print '########i########',i
                sample_input_dir = DIR + str(i)
                input_dir = DIR + str(i+1)
                [previous_popular_map,previous_popular_half_map]= sim2(sample_input_dir,input_dir,previous_popular_map,previous_popular_half_map)
if __name__ == '__main__':
    main()
    
