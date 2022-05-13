###

# Libraries
import pandas as pd
import os
import sys
import time

# Functions
def categorize_line(message):
    '''
    :param message: string, message component of line in log
    :return: categorization of line
    '''
    if 'Start Serving' in message:
        return 'started'
    elif 'Serving Complete' in message:
        return 'completed'
    elif message.startswith("['order"):
        return 'ordered'
    elif 'Serving: Dispensing Warning' in message:
        return 'dispense warning'
    elif 'Serving: Out Of Stock' in message:
        return 'out of stock warning'
    elif 'Serving Stopped' in message:
        return 'stopped'
    elif 'Serving: Out of Stock' in message:
        return 'out of stock'
    elif 'Fill Tube Start' in message:
        return 'started fill'
    elif 'Fill Tube Complete' in message:
        return 'finished fill'
    elif 'TypeError' in message or 'KeyError' in message:
        return 'python error'
    elif 'Sync recipe failed' in message:
        return 'sync error'
    elif 'Cleaning' in message or 'Circular' in message or 'Draining' in message or 'Empty Tubes' in message:
        return 'cleaning'
    elif 'Wait' in message:
        return 'wait'
    elif 'Fill Tube: No Flow Detected' in message:
        return 'no flow warning'
    elif 'Cloudbar login error' in message:
        return 'login error'
    elif 'Lost connection' in message:
        return 'lost connection'
    elif 'update package' in message or 'useless files' in message or 'Update' in message:
        return 'updating'
    elif 'Start Calibration' in message or 'Calibration Complete' in message:
        return 'calibration'
    elif 'ordering_rety' in message:
        return 'retry order'
    elif 'customer_service_request' in message:
        return 'requested customer service'
    elif 'fill_tube_start' in message:
        return 'selected fill tube'
    elif 'weekly_cleaning_start' in message:
        return 'selected cleaning'
    else:
        return 'unknown'


def parse_lines(filename):
    '''
    :param filename: filename
    :return: tuple of (filename, parsed results)
    '''
    results = []
    with open(filename, 'r') as file:
        for i, line in enumerate(file):
            line = line.strip()
            if len(line) == 0:
                continue
            try:
                ts_str, line_type, message = line.split(' - ')
            except:
                print(f'Failure loading line {i+1} in file {filename}. Text:\n', line)
                continue
            # "Log events"
            if message.startswith('---') or message.startswith("['--") \
                or line_type=='[CRITICAL]' or message.startswith("['order:") \
                or 'fill_tube_start' in message or 'weekly_cleaning_start' in message:
                # Categorize message
                line_is = categorize_line(message)
                if line_is == 'unknown':
                    results.append((i, ts_str, 'UNKNOWN', line.strip()))
                elif line_is == 'stopped':
                    reason = eval(message)[1]
                    results.append((i, ts_str, line_is, reason))
                else:
                    results.append((i, ts_str, line_is))
    return (filename, results)


def parse_all_logs(root_dir):
    '''
    :param root_dir: root directory
    :return: list of parsed logs
    '''
    all_results = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.startswith('log') and 'checkpoint' not in file:
                filename = os.path.join(root, file)
                _, parsed_results = parse_lines(filename)
                if len(parsed_results) == 0:
                    continue
                all_results.append((filename, parsed_results))
    return all_results


def get_actions(log_lines):
    '''
    :param log_lines: processed and relevant lines from log
    :return: indices where user actions take place (see set in if statement for actions)
    '''
    action_inds = []
    for i, line in enumerate(log_lines):
        if line[2] in {'ordered', 'retry order',
                       'selected fill tube', 'selected cleaning',
                       'requested customer service'}:
            action_inds.append(i)
    return action_inds


def action_results(log_lines, action_inds, outcomes={'stopped', 'completed', 'finished fill'}):
    '''
    :param log_lines: processed and relevant lines from log
    :param action_inds: indices of user actions in log_lines
    :param outcomes: events considered outcomes (from user actions)
    :return:
    '''
    events = []
    for i in range(len(action_inds) - 1):
        ind = action_inds[i]
        next_ind = action_inds[i + 1]
        action_line, action_type = log_lines[ind][0], log_lines[ind][2]
        for j in range(ind + 1, next_ind):
            line = log_lines[j]
            line_num, ts, event = line[0], line[1], line[2]
            if event in outcomes:
                if event == 'stopped':
                    event += f' - {line[3]}'
                events.append((action_type, action_line, event, line_num))
    return events


def get_orders_incompletes(action_result_list):
    '''
    :param action_result_list: list of action-outcome pairs for log
    :return: number of orders, number of incomplete orders
    '''
    num_orders = 0
    num_incomplete = 0
    for action, action_line, result, result_line in action_result_list:
        if action in ['ordered', 'retry order']:
            num_orders += 1
            if 'stopped' in result:
                num_incomplete += 1
    return num_orders, num_incomplete


def get_self_resolved(action_result_list):
    '''
    :param action_result_list: list of action-outcome pairs for log
    :return: number of self-resolved orders
    '''
    first_item = action_result_list[0]
    prev_action, prev_result = first_item[0], first_item[2]
    self_resolved = 0
    for action, action_line, result, result_line in action_result_list[1:]:
        if prev_result == 'stopped - dispensing warning' \
                and action in ['ordered', 'retry order'] \
                and result == 'completed':
            self_resolved += 1
        prev_action = action
        prev_result = result
    return self_resolved


def get_dir_stats(results):
    '''
    :param results: output from parse_all_logs
    :return: total orders, total incomplete, total self resolved, dataframe with stats for each file
    '''
    total_orders = 0
    total_incomplete = 0
    total_self_resolved = 0
    stats_by_file = []
    for result in results:
        file, log_lines = result
        action_inds = get_actions(log_lines)
        action_result_logs = action_results(log_lines, action_inds)
        # Get number of orders and dispense incompletes
        num_orders, num_incomplete = get_orders_incompletes(action_result_logs)
        self_resolved = get_self_resolved(action_result_logs)
        # Aggregate
        total_orders += num_orders
        total_incomplete += num_incomplete
        total_self_resolved += self_resolved
        stats_by_file.append({'filename': file,
                              'num_orders': num_orders,
                              'num_incomplete': num_incomplete,
                              'self_resolved': self_resolved})
    return total_orders, total_incomplete, total_self_resolved, pd.DataFrame(stats_by_file)


# Main program
if __name__ == '__main__':
    start = time.time()
    log_dir, stats_file = sys.argv[1], sys.argv[2]
    parsing_results = parse_all_logs(log_dir)
    orders, incomplete, resolved, log_df = get_dir_stats(parsing_results)
    print('Total number of orders:', orders)
    print(f'Percentage of incomplete orders: {incomplete/orders*100:.2f}%')
    print('Number of dispense warnings resolved after 1 reorder:', resolved)
    log_df.to_csv(stats_file, index=False)
    print('CSV file of log stats saved at:', stats_file)
    print(f'Script ran in {int(time.time()-start)} seconds')
