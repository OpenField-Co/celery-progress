import datetime
import logging
import re
from abc import ABCMeta, abstractmethod

from celery.result import EagerResult, allow_join_result
from celery.backends.base import DisabledBackend

logger = logging.getLogger(__name__)

PROGRESS_STATE = 'PROGRESS'


class AbstractProgressRecorder(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def set_progress(self, current, total, description=""):
        pass


class ConsoleProgressRecorder(AbstractProgressRecorder):

    def set_progress(self, current, total, description=""):
        print('processed {} items of {}. {}'.format(current, total, description))


class ProgressRecorder(AbstractProgressRecorder):

    def __init__(self, task):
        self.task = task
        self.start_time = datetime.datetime.now()

    def set_progress(self, current: int, total: int, description: str=""):
        state = PROGRESS_STATE
        meta = {
            'pending': False,
            'current': current,
            'total': total,
            'percent': self._percent_float(current, total),
            'percent_int': self._percent_int(current, total),
            'description': description,
            'start_time': self.start_time,
            'est_time_remaining_s': self._est_time(current, total, self.start_time)
        }
        self.task.update_state(
            state=state,
            meta=meta
        )
        return state, meta

    def _percent_float(self, current: int, total: int):
        if current == 0 or total == 0:
            return 0.0
        percent = float(round(((current / total) * 100.0), 2))
        return 1.0 if (percent == 0 and current > 0) else percent

    def _percent_int(self, current: int, total: int):
        if current == 0 or total == 0:
            return 0
        percent = int((current / total) * 100.0)
        return 1 if (percent == 0 and current > 0) else percent

    def _est_time(self, current: int, total: int, start_time: datetime.datetime):
        if current == 0 or total == 0:
            return -1
        curr_time = datetime.datetime.now()
        diff_time = curr_time - start_time
        diff_s = float(diff_time.seconds)
        perc = ((current / total) * 100.0)
        time_total_s = (diff_s * (100.0 / perc))
        time_remain_s = int(time_total_s - diff_s)
        return time_remain_s

class Progress(object):

    def __init__(self, result):
        """
        result:
            an AsyncResult or an object that mimics it to a degree
        """
        self.result = result

    def get_info(self):
        task_meta = self.result._get_task_meta()
        state = task_meta["status"]
        info = task_meta["result"]
        response = {'state': state}
        if self.result.result is None:
            response.update({
                'complete': True,
                'success': None,
                'progress': _get_unknown_progress(state),
            })
            return response
        if state in ['SUCCESS', 'FAILURE']:
            success = self.result.successful()
            with allow_join_result():
                response.update({
                    'complete': True,
                    'success': success,
                    'progress': _get_completed_progress(),
                    'result': self.result.get(self.result.id) if success else str(info),
                })
        elif state in ['RETRY', 'REVOKED']:
            if state == 'RETRY':
                # in a retry sceneario, result is the exception, and 'traceback' has the details
                # https://docs.celeryq.dev/en/stable/userguide/tasks.html#retry
                traceback = task_meta.get("traceback")
                seconds_re = re.search("Retry in \d{1,10}s", traceback)
                if seconds_re:
                    next_retry_seconds = int(seconds_re.group()[9:-1])
                else:
                    next_retry_seconds = "Unknown"

                result = {"next_retry_seconds": next_retry_seconds, "message": f"{str(task_meta['result'])[0:50]}..."}
            else:
                result = 'Task ' + str(info)
            response.update({
                'complete': True,
                'success': False,
                'progress': _get_completed_progress(),
                'result': result,
            })
        elif state == 'IGNORED':
            response.update({
                'complete': True,
                'success': None,
                'progress': _get_completed_progress(),
                'result': str(info)
            })
        elif state == PROGRESS_STATE:
            response.update({
                'complete': False,
                'success': None,
                'progress': info,
            })
        elif state in ['PENDING', 'STARTED']:
            response.update({
                'complete': False,
                'success': None,
                'progress': _get_pending_progress(state),
            })
        else:
            logger.error('Task %s has unknown state %s with metadata %s', self.result.id, state, info)
            response.update({
                'complete': True,
                'success': False,
                'progress': _get_unknown_progress(state),
                'result': 'Unknown state {}'.format(state),
            })
        return response


class KnownResult(EagerResult):
    """Like EagerResult but supports non-ready states."""
    def __init__(self, id, ret_value, state, traceback=None):
        """
        ret_value:
            result, exception, or progress metadata
        """
        # set backend to get state groups (like READY_STATES in ready())
        self.backend = DisabledBackend
        super().__init__(id, ret_value, state, traceback)

    def ready(self):
        return super(EagerResult, self).ready()

    def __del__(self):
        # throws an exception if not overridden
        pass


def _get_completed_progress():
    return {
        'pending': False,
        'current': 100,
        'total': 100,
        'percent': 100.0,
        'percent_int': 100,
        'start_time': None,
        'est_time_remaining_s': 0
    }


def _get_unknown_progress(state):
    return {
        'pending': False,
        'current': 0,
        'total': 100,
        'percent': None,
        'percent_int': None,
        'start_time': None,
        'est_time_remaining_s': None
    }


def _get_pending_progress(state):
    return {
        'pending': True,
        'current': 0,
        'total': 100,
        'percent': -1.0,
        'percent_int': -1,
        'start_time': None,
        'est_time_remaining_s': -1
    }
