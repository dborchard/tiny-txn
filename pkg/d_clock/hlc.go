package clock

type HlcClock struct {
}

var _ Clock = (*HlcClock)(nil)

func (h HlcClock) Now() int64 {
	panic("implement me")
}
