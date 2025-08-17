package hajobs

const (
	CronMinutely            = "* * * * *"
	CronHourly              = "0 * * * *"
	CronDaily               = "0 0 * * *"
	CronMonthly             = "0 0 0 1 * * *"
	CronEveryFiveMinutes    = "*/5 * * * *"
	CronEveryFifteenMinutes = "*/15 * * * *"
	CronEveryTenSeconds     = "*/10 * * * * * *"
	CronNightlyAt3          = "0 3 * * *"
	CronNightlyAt2          = "0 2 * * *"
	CronNightlyAt4          = "0 4 * * *"
	CronNightlyAt5          = "0 5 * * *"
	CronNightlyAt6          = "0 6 * * *"
	CronNightlyAt7          = "0 7 * * *"
)
