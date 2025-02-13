import argparse
from deliverables.clients import ALL_DELIVERIES, ALL_POST_DELIVERIES
from deliverables.promoter import DeliveryPromoter
from deliverables.runner import DeliveryRunner
from deliverables.constants import Frequencies, RunModes, DAILY_BATCHTIME
from deliverables.utils.filter_deliveries import filter_deliveries_by_step_type, filter_deliveries_by_input_table
from deliverables.clients.client_utils.groups import filter_deliveries_group
from deliverables.utils.common import get_run_id, DagType
from deliverables.utils.runner_utils import create_status_table
from datetime import date


def parse_freq_args(arg, valid_vals):
    if arg is None:
        return None
    parsed = [val.strip().lower() for val in arg.split(",")]
    assert len(parsed) == 1, 'should pass single frequency: daily, weekly, monthly'
    assert parsed[0] in list(valid_vals.keys()), 'pass in frequency: daily, weekly, monthly'
    return valid_vals[parsed[0]]


def split_check_len(value):
    split = value.split('.')
    assert len(split) == 2, 'should pass deliverable with client.x format'
    return split


def parse_client_delivery(val, val_deliveries, val_post_deliveries, valid_deliveries, valid_post_deliveries,
                          post=False):

    assert len(val.split('.')) in [1, 2], 'the only valid delivery formats are client or client.index'

    if len(val.split('.')) == 1:
        # Case: we have a client name
        if not post:
            for v in valid_deliveries:
                if v.client == val:
                    val_deliveries.append(v)
        for v in valid_post_deliveries:
            if v.client == val:
                val_post_deliveries.append(v)

    elif len(val.split('.')) == 2:
        # Case: we have a delivery name
        if len(val.split('-')) > 1:
            l, r = val.split('-')
            lsplit = split_check_len(l)
            assert len(r.split('.')) == 1, 'should pass index value only in the upper index'
            assert int(r) > int(lsplit[1]), 'should pass upper index strictly greater than lower'
            for ind in range(int(lsplit[1]), int(r) + 1):
                if not post:
                    for v in valid_deliveries:
                        if v.client == lsplit[0] and v.index == int(ind):
                            val_deliveries.append(v)
                else:
                    for v in valid_post_deliveries:
                        if v.client == lsplit[0] and v.index == int(ind):
                            val_post_deliveries.append(v)

        else:
            split = split_check_len(val)
            if not post:
                for v in valid_deliveries:
                    if v.client == split[0] and v.index == int(split[1]):
                        val_deliveries.append(v)
            else:
                for v in valid_post_deliveries:
                    if v.client == split[0] and v.index == int(split[1]):
                        val_post_deliveries.append(v)
    return val_deliveries, val_post_deliveries

def parse_deliverables_arg(arg, valid_deliveries_all, current_deliveries_all):
    valid_deliveries, valid_post_deliveries = valid_deliveries_all['deliveries'], \
                                              valid_deliveries_all['post_deliveries']
    current_deliveries, current_post_deliveries = current_deliveries_all['deliveries'], \
                                                  current_deliveries_all['post_deliveries']
    if arg is None:
        return [v.delivery for v in current_deliveries], [v.delivery for v in current_post_deliveries]
    parsed = []
    parsed_post = []
    for val in arg.split(","):
        val = val.strip()
        mode = 'add'
        if val.startswith('+'):
            val = val.strip('+')
        elif val.startswith('-'):
            mode = 'remove'
            val = val.strip('-')
        elif val.startswith('*'):
            mode = 'filter'
            val = val.strip('*')

        val_deliveries = []
        val_post_deliveries = []

        base_delivery_list = valid_deliveries if mode in ['remove', 'filter'] else current_deliveries
        base_post_delivery_list = valid_post_deliveries if mode in ['remove', 'filter'] else current_post_deliveries

        if val == 'ALL':
            val_deliveries = base_delivery_list
            val_post_deliveries = base_post_delivery_list
        elif val.startswith('group__'):
            # Case: we have a custom group name
            val_deliveries = filter_deliveries_group(base_delivery_list, val.replace('group__', ''))
        elif val.startswith('table__'):
            # Case: we have a table name
            val_deliveries = filter_deliveries_by_input_table(base_delivery_list, [val.replace('table__', '')])
        elif val.startswith('steptype__'):
            # Case: we have a step type
            val_deliveries = filter_deliveries_by_step_type(base_delivery_list, [val.replace('steptype__', '')])
        elif val.startswith('post_delivery__'):
            val_deliveries, val_post_deliveries = parse_client_delivery(val.replace('post_delivery__', ''),
                                                                        val_deliveries, val_post_deliveries,
                                                                        valid_deliveries, valid_post_deliveries,
                                                                        post=True)
        else:
            # Case: we have a client / delivery name
            val_deliveries, val_post_deliveries = parse_client_delivery(val, val_deliveries, val_post_deliveries,
                                                                        valid_deliveries, valid_post_deliveries)

        old_len = len(parsed)
        old_len_post = len(parsed_post)
        if mode == 'add':
            parsed = list(dict.fromkeys(parsed + val_deliveries))
            print(f"{len(parsed) - old_len} deliveries added due to {val} element")
            parsed_post = list(dict.fromkeys(parsed_post + val_post_deliveries))
            print(f"{len(parsed_post) - old_len_post} post-deliveries added due to {val} element")
        elif mode == 'remove':
            parsed = [p for p in parsed if p not in val_deliveries]
            print(f"{- len(parsed) + old_len} deliveries removed due to {val} element")
            if val.startswith('post_delivery'):
                parsed_post = [p for p in parsed_post if p not in val_post_deliveries]
                print(f"{- len(parsed_post) + old_len_post} post-deliveries removed due to {val} element")
            else:
                parsed_post = []
                print(f"{- len(parsed_post) + old_len_post} post-deliveries removed due to {val} element")
        elif mode == 'filter':
            parsed = [p for p in parsed if p in val_deliveries]
            print(f"{- len(parsed) + old_len} deliveries removed due to {val} element")
            parsed_post = []
            print(f"{- len(parsed_post) + old_len_post} post-deliveries removed due to {val} element")
        else:
            raise Exception(f"Invalid mode {mode}")
    return [p.delivery for p in parsed], [p.delivery for p in parsed_post]


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--runmode")
    argparser.add_argument("--deliverables")
    argparser.add_argument("--frequency")
    argparser.add_argument("--number_of_threads")
    argparser.add_argument("--promote", action="store_true")
    argparser.add_argument("--skip_auto_cleanup", action="store_true")
    argparser.add_argument("--use_dev_pipeline_service", action="store_true")
    argparser.add_argument("--custom_pipeline_image")
    argparser.add_argument("--data_version")
    argparser.add_argument("--force_daily_batchtime")
    argparser.add_argument("--store_nb_tables", action="store_true")
    argparser.add_argument("--skip_promote", action="store_true")
    args = argparser.parse_args()

    # Parse Run Modes
    def parse_runmode_args(arg, valid_vals):
        if arg is None:
            return RunModes.DEBUG
        parsed = [val.strip().lower() for val in arg.split(",")]
        assert len(parsed) == 1, 'should only pass a single run mode: prod, opti_only, test, debug or table_check'
        assert parsed[0] in list(valid_vals.keys()), 'pass in runmode: prod, opti_only, test, debug or table_check'
        return valid_vals[parsed[0]]
    valid_runmodes = {
        'prod': RunModes.PROD,
        'opti_only': RunModes.OPTI_ONLY,
        'test': RunModes.TEST,
        'debug': RunModes.DEBUG,
        'table_check': RunModes.TABLE_CHECK
    }
    runmode = parse_runmode_args(args.runmode, valid_runmodes)
    opti_only = runmode == RunModes.OPTI_ONLY
    test = runmode == RunModes.TEST
    debug = runmode == RunModes.DEBUG
    table_check = runmode == RunModes.TABLE_CHECK
    run = not (args.promote or table_check)
    promote = not args.skip_promote and (args.promote or test or debug or opti_only or args.frequency == Frequencies.DAY or args.frequency == Frequencies.WEEK)
    number_of_threads = int(args.number_of_threads)

    # Parse frequencies
    valid_freqs = {
        'daily': Frequencies.DAY,
        'weekly': Frequencies.WEEK,
        'monthly': Frequencies.MONTH
    }
    freq = parse_freq_args(args.frequency, valid_freqs)

    # Parse deliverables
    deliveries_map = {'deliveries': ALL_DELIVERIES, 'post_deliveries': ALL_POST_DELIVERIES if promote else []}
    valid_deliveries_all = {k: list(dict.fromkeys([pd for pd in deliveries_map[k]
                                if pd.frequency == freq and
                                (pd.delivery_day is None or pd.delivery_day == date.today().strftime("%A"))]))
                            for k in ['deliveries', 'post_deliveries']}

    current_deliveries_all = {k: [v for v in valid_deliveries_all[k]
                                  if not (v.client.startswith('test') or v.client.startswith('mock') or v.client == 'template')]
                            for k in ['deliveries', 'post_deliveries']}
    deliveries_nu, post_deliveries_nu = parse_deliverables_arg(args.deliverables, valid_deliveries_all,
                                                               current_deliveries_all)
    deliveries = list(dict.fromkeys(deliveries_nu))
    post_deliveries = list(dict.fromkeys(post_deliveries_nu))

    run_id = get_run_id(DagType.MONTHLY.value)

    # Load in cache
    if not (debug or test or table_check):
        create_status_table(test, run_id)

    success = True
    daily_batchtime = args.force_daily_batchtime if args.force_daily_batchtime else DAILY_BATCHTIME


    if run:
        runner = DeliveryRunner(run_id,
                                use_dev_pipeline_service=args.use_dev_pipeline_service,
                                custom_pipeline_image=args.custom_pipeline_image,
                                data_version=args.data_version if args.data_version else "current",
                                data_recency="monthly" if freq == Frequencies.MONTH else "weekly",
                                daily_batchtime=daily_batchtime)
        success, _ = runner.run(deliveries, debug=debug, test=test,
                                                           nb_max_thread=number_of_threads, custom=False,
                                                           optimization_only=opti_only,
                                                           auto_cleanup=not args.skip_auto_cleanup)
        if not success:
            print("This DAG run failed!")

    if promote:
        promoter = DeliveryPromoter(run_id)
        promote_success = promoter.run(deliveries, nb_max_thread = number_of_threads, debug=debug, test=test,
                                       custom=False, optimization_only=opti_only, daily_batchtime=daily_batchtime)
        if not promote_success:
            print("Delivery promotion failed!")
        success = success and promote_success

    if not success:
        raise Exception('DAG run and/or delivery promotion failed!')
