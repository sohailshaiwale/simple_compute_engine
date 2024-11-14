import csv
from functools import lru_cache


def read_csv(file_path, delimiter=',', has_header=True):
    def read():
        with open(file_path, 'r') as f:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(f.read(1024))
            f.seek(0)
            reader = csv.reader(f, dialect=dialect, delimiter=delimiter)
            
            if has_header:
                headers = next(reader)
                rows = list(reader)
            else:
                rows = list(reader)
                headers = [f"col_{i+1}" for i in range(len(rows[0]))]

        return [dict(zip(headers, row)) for row in rows]
    
    return Dataframe([('read', read)])


class GroupBy:
    def __init__(self, operations, column_name):
        self.operations = operations
        self.column_name = column_name

# Needs improvement
    def count(self):
        def count_op(data):
            res = dict()
            for item in data:
                key = item[self.column_name]            
                res[key] = res.get(key, 0) + 1
            data = []
            for key, value in res.items():
                temp = dict() 
                temp[self.column_name] = key
                temp['count'] = value
                data.append(temp)
            return data
        return Dataframe(self.operations+ [('count', count_op)]) 


    def sum(self, column_name):
        def sum_op(data):
            sums = {}
            for row in data:
                try:
                    value = float(row[column_name])
                    key = row[self.column_name]
                    sums[key] = sums.get(column_name, 0) + value 
                except ValueError:
                    print(f'{column_name} of not type int or float')
                    return None
            data = []
            for key, value in sums.items():
                temp = dict()
                temp[self.column_name] = key
                temp['sum'] = value
                data.append(temp)
            return data
        return Dataframe(self.operations + [('sum', sum_op)])

class Dataframe:
    def __init__(self, operations):
        self.operations = operations
        self._cached_data = None

    def columns(self):
        def columns_op(data):
            return list(data[0].keys()) if isinstance(data, list) else list(data.keys())
        return Dataframe(self.operations + [('columns', columns_op)])

    def head(self, n=5):
        def head_op(data):
            if isinstance(data, dict):
                items = list(data.items())[:n]
                return dict(items)
            return data[:n]
        return Dataframe(self.operations + [('head', head_op)])
    
    def filter(self, column_name, value):
        def filter_op(data):
            if isinstance(data, dict):
                return {k: v for k, v in data.items() if k == value}
            return [row for row in data if row[column_name] == value]
        return Dataframe(self.operations + [('filter', filter_op)])
    
    def select(self, *column_names):
        def select_op(data):
            if isinstance(data, dict):
                # For grouped data, create a new dict with only selected keys
                return {k: v for k, v in data.items() if k in column_names}
            return [{col: row[col] for col in column_names} for row in data]
        return Dataframe(self.operations + [('select', select_op)])
        
    def group_by(self, column_name):
        return GroupBy(self.operations, column_name)

    def drop(self, column_name):
        def drop_op(data):
            if isinstance(data, dict):
                data.pop(column_name)
            else:
                data = [{k: v for k, v in row.items() if k != column_name} for row in data]
            return data
        return Dataframe(self.operations + [('drop', drop_op)])

    def withColumnRenamed(self, old_name, new_name):
        def withColumnRenamed_op(data):
            if isinstance(data, dict):
                data[new_name] = data.pop(old_name)
            else:
                data = [{new_name if k == old_name else k: v for k, v in row.items()} for row in data]
            return data
        return Dataframe(self.operations + [('withColumnRenamed', withColumnRenamed_op)])

    def sort(self, column_name, ascending=True):
        def sort_op(data):
            return sorted(data, key=lambda x: x[column_name], reverse=not ascending)
        return Dataframe(self.operations + [('sort', sort_op)])
    
    def join(self, other, on, join_type='inner'):
        def join_op(data1):
            # Get data from other dataframe
            data2 = None
            for _, operation in other.operations:
                data2 = operation(data2) if data2 is not None else operation()
            
            # Create lookups
            lookup1 = {item[on]: item for item in data1}
            lookup2 = {item[on]: item for item in data2}
            all_keys = set(lookup1.keys()).union(lookup2.keys())
            
            result = []
            if join_type == 'inner':
                for item in data1:
                    key = item.get(on)
                    if key in lookup2:
                        result.append({**item, **lookup2[key]})
                        
            elif join_type == 'left':
                for item in data1:
                    key = item.get(on)
                    if key in lookup2:
                        result.append({**item, **lookup2[key]})
                    else:
                        result.append(item)
                        
            elif join_type == 'right':
                for item in data2:
                    key = item.get(on)
                    if key in lookup1:
                        result.append({**lookup1[key], **item})
                    else:
                        result.append(item)
                        
            elif join_type == 'outer':
                for key in all_keys:
                    item1 = lookup1.get(key, {})
                    item2 = lookup2.get(key, {})
                    result.append({**item1, **item2})
                    
            return result
        
        return Dataframe(self.operations + [('join', join_op)])
   

    def with_column(self, column_name, operation, value):
        def with_column_op(data):
            if operation == '+':
                for item in data:
                    try:
                        column_value = int(item[column_name])
                    except ValueError:
                        print(f'{column_name} not int or float')
                        
                    if isinstance(column_value, (int, float)) and isinstance(value, (int, float)):
                        item[column_name] = column_value + value

                    elif isinstance(column_value ,str) and isinstance(value, str):
                        item[column_name] = item[column_name] + value

                    else:
                        raise TypeError("Incomaptible data types")
            
            elif operation == '-':
                for item in data:
                    try:
                        column_value = int(item[column_name])
                    except ValueError:
                        print(f'{column_name} not int or float')
                        
                    if isinstance(column_value, (int, float)) and isinstance(value, (int, float)):
                        item[column_name] = column_value - value

                    elif isinstance(column_value ,str) and isinstance(value, str):
                        item[column_name] = item[column_name] - value

                    else:
                        raise TypeError("Incomaptible data types")
            elif operation == '*':
                for item in data:
                    try:
                        column_value = int(item[column_name])
                    except ValueError:
                        print(f'{column_name} not int or float')
                        
                    if isinstance(column_value, (int, float)) and isinstance(value, (int, float)):
                        item[column_name] = column_value*value

                    elif isinstance(column_value ,str) and isinstance(value, str):
                        item[column_name] = item[column_name]*value

                    else:
                        raise TypeError("Incomaptible data types")
            elif operation == '/':
                for item in data:
                    try:
                        column_value = int(item[column_name])
                    except ValueError:
                        print(f'{column_name} not int or float')
                        
                    if isinstance(column_value, (int, float)) and isinstance(value, (int, float)):
                        item[column_name] = column_value//value

                    elif isinstance(column_value ,str) and isinstance(value, str):
                        item[column_name] = item[column_name]//value

                    else:
                        raise TypeError("Incomaptible data types")
            return data 
        return Dataframe(self.operations + [('with_column', with_column_op)])
    

    @lru_cache(maxsize = 1000)
    def cache(self):
        def cache_op(data):
            return data
        return Dataframe(self.operations + [('cache', cache_op)])

    def unpersist(self):
        if 'cache' not in [op[0] for op in self.operations]:
            raise ValueError("Cannot call unpersist before cache")
        def unpersist_op(data):
            print(self.cache.cache_info().currsize)
            self.cache.cache_clear()
            print(self.cache.cache_info().currsize)
            return data
        return Dataframe(self.operations + [('unpersist', unpersist_op)])

    def _execute_operations(self):
        # Execute only if not cached
        if self._cached_data is None:
            data = None
            for _, operation in self.operations:
                print(_, operation)
                data = operation(data) if data is not None else operation()
            self._cached_data = data
        return self._cached_data

    def __str__(self):
        return str(self._execute_operations())

    def write_csv(self, file_path, delimiter=',', write_header=True):
        data = self._execute_operations()
        
        # Write the final data to CSV
        with open(file_path, 'w', newline='') as f:
            if isinstance(data, dict):
                # Handle grouped data
                writer = csv.writer(f, delimiter=delimiter)
                if write_header:
                    writer.writerow(['key', 'value'])
                for key, value in data.items():
                    writer.writerow([key, value])
            else:
                # Handle regular data
                writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter=delimiter)
                if write_header:
                    writer.writeheader()
                writer.writerows(data)
    


if __name__ == "__main__":
    df = read_csv('data.csv')
    df = df.sort('Index')
    df = df.withColumnRenamed('Index', 'id')
    df = df.select('id', 'First Name', 'Last Name')
    df = df.with_column('id', '-', 5)
    df = df.group_by('First Name').sum('id')
    df = df.cache()
    df = df.unpersist()
    print(df)
