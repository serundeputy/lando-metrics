# Lando Metrics Analysis

Get, record, and measure growth of Lando metrics.

## Getting the Metrics

Create a `.env` file with the QBox credentials:

```bash
QBOX_USER=<yourQboxUser>
QBOX_PASSWORD=<yourQboxPassword>
```

Start the app:

```bash
lando start
```

Currently there are hard coded lists for `months` and `providers` you wish to get the metrics for in the `app.py` file. So, if you want to adjust, add, or remove time periods and/or providers that you are interested in you need to edit those lists.

To run the app:

```bash
lando python app.py
```
