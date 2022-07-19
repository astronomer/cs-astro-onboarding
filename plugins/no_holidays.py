import holidays
from datetime import timedelta
from typing import Optional

from pendulum import Date, DateTime, Time, timezone

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


UTC = timezone("UTC")
COUNTRY = "PL"


class NoPublicHolidaysTimetable(Timetable):  # If it's a public holiday - do not run. Otherwise, run at midnight UTC.

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is None:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            # earliest: The earliest time the DAG may be scheduled.
            # This is a pendulum.DateTime calculated from all the start_date arguments from the DAG and its tasks
            if next_start is None:  # No start_date = don't schedule.
                return None
            if not restriction.catchup:  # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            elif next_start.time() != Time.min:  # If start_date does not fall on midnight, skip to the next day.
                next_start = DateTime.combine((next_start.date() + timedelta(days=1)), Time.min).replace(tzinfo=UTC)
        else:  # There was a previous run on the regular schedule.
            next_start = last_automated_data_interval.start + timedelta(days=1)

        current_year = next_start.year
        holidays_dates = list(holidays.country_holidays(COUNTRY, years=current_year).keys())
        holidays_dt = [DateTime.combine(h, Time.min).replace(tzinfo=UTC) for h in holidays_dates]
        while DateTime.combine(next_start, Time.min).replace(tzinfo=UTC) in holidays_dt:
            next_start = next_start + timedelta(days=1)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.

        return DagRunInfo.interval(start=next_start, end=(next_start + timedelta(days=1)))

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        run_after = run_after - timedelta(days=1)
        current_year = run_after.year
        holidays_dates = list(holidays.country_holidays(COUNTRY, years=current_year).keys())
        holidays_dt = [DateTime.combine(h, Time.min).replace(tzinfo=UTC) for h in holidays_dates]
        while run_after in holidays_dt:
            run_after = run_after - timedelta(days=1)
        return DataInterval(start=run_after, end=(run_after + timedelta(days=1)))


class NoPublicHolidaysTimetablePlugin(AirflowPlugin):
    name = "no_public_holidays_timetable_plugin"
    timetables = [NoPublicHolidaysTimetable]
