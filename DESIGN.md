# Burrow

This repo is a fork from the linkedin project. It analyses and exposes metrics
over http. Then a custom datadog check is consuming it an produces `burrow.*`
metrics.

At this moment those metrics are not used in monitor/dashboard. But in the
future services/people should rely on it. At this time it could makes sense to
move the level from business hours to something more critical. A downtime could
page teams for nothing.

As it's just a fork turned into a gygservice, if you want to know more about how
it works, please read the readme or developers documentation.
