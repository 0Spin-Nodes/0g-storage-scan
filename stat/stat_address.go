package stat

import (
	"time"

	"github.com/0glabs/0g-storage-scan/store"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type StatAddress struct {
	*BaseStat
	statType string
}

func MustNewStatAddress(cfg *StatConfig, db *store.MysqlStore, sdk *web3go.Client, startTime time.Time) *AbsStat {
	baseStat := &BaseStat{
		Config:    cfg,
		DB:        db,
		Sdk:       sdk,
		StartTime: startTime,
	}

	statAddress := &StatAddress{
		BaseStat: baseStat,
		statType: baseStat.Config.MinStatIntervalDailyAddress,
	}

	return &AbsStat{
		Stat: statAddress,
		sdk:  baseStat.Sdk,
	}
}

func (sa *StatAddress) nextTimeRange() (*TimeRange, error) {
	lastStat, err := sa.DB.AddressStatStore.LastByType(sa.statType)
	if err != nil {
		return nil, err
	}

	var nextRangeStart time.Time
	if lastStat == nil {
		nextRangeStart = sa.StartTime
	} else {
		t := lastStat.StatTime.Add(store.Intervals[sa.statType])
		nextRangeStart = t
	}

	timeRange, err := sa.calStatRange(nextRangeStart, store.Intervals[sa.statType])
	if err != nil {
		return nil, err
	}

	return timeRange, nil
}

func (sa *StatAddress) calculateStat(tr TimeRange) error {
	stat, err := sa.statByTimeRange(tr)
	if err != nil {
		return err
	}

	hrs, err := sa.calStatRangeStart(tr.end, store.Hour)
	if err != nil {
		return err
	}
	hStat, err := sa.statByTimeRange(TimeRange{hrs, tr.end, store.Hour})

	drs, err := sa.calStatRangeStart(tr.end, store.Day)
	if err != nil {
		return err
	}
	dStat, err := sa.statByTimeRange(TimeRange{drs, tr.end, store.Day})

	stats := []*store.AddressStat{stat, hStat, dStat}
	return sa.DB.DB.Transaction(func(dbTx *gorm.DB) error {
		if err := sa.DB.AddressStatStore.Del(dbTx, hStat); err != nil {
			return errors.WithMessage(err, "failed to del hour stat")
		}
		if err := sa.DB.AddressStatStore.Del(dbTx, dStat); err != nil {
			return errors.WithMessage(err, "failed to del day stat")
		}
		if err := sa.DB.AddressStatStore.Add(dbTx, stats); err != nil {
			return errors.WithMessage(err, "failed to save stats")
		}
		return nil
	})
}

func (sa *StatAddress) statByTimeRange(tr TimeRange) (*store.AddressStat, error) {
	delta, err := sa.DB.AddressStore.Count(tr.start, tr.end)
	if err != nil {
		return nil, err
	}

	preTimeRange, err := sa.calStatRange(tr.start, -store.Intervals[tr.intervalType])
	if err != nil {
		return nil, err
	}

	var addressStat store.AddressStat
	exist, err := sa.DB.AddressStatStore.Exists(&addressStat, "stat_type = ? and stat_time = ?",
		tr.intervalType, preTimeRange.start)
	if err != nil {
		logrus.WithError(err).Error("Failed to query databases")
		return nil, err
	}

	var total uint64
	if !exist {
		total = 0
	} else {
		total = addressStat.AddrTotal
	}

	return &store.AddressStat{
		StatTime:   tr.start,
		StatType:   sa.statType,
		AddrCount:  delta,
		AddrActive: 0,
		AddrTotal:  total + delta,
	}, nil
}
