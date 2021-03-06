# Gas SDK  

The SDK of Gas APIs.

## Usage

```golang
import (
	qgas "github.com/qiniupd/qiniu-go-sdk/gas"
)

func main() {

	config := qgas.Config{
		MinerID:   "f23423",
		AccessKey: <Qiniu AccessKey>,
		SecretKey: <Qiniu SecretKey>,
	}

	g := qgas.NewQGas(&config)

	// do sealing
	sealingID := "2334232"

	g.StartAction(sealingID, qgas.ActionPreSealSector) // g.StartAction(<SealingID>, <Action>)
	g.EndAction(sealingID, qgas.ActionPreSealSector) // g.EndAction(<SealingID>, <Action>)
	g.Wait(sealingID, qgas.ActionProveCommitSector) // g.Wait(<SealingID>, <Action>)
	g.CancelSealing(sealingID) // g.CancelSealing(<SealingID>)
}
```
