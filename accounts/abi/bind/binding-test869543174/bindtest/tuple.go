// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package bindtest

import (
	"math/big"
	"strings"

	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/accounts/abi"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// TupleP is an auto generated low-level Go binding around an user-defined struct.
type TupleP struct {
	X uint8
	Y uint8
}

// TupleQ is an auto generated low-level Go binding around an user-defined struct.
type TupleQ struct {
	X uint16
	Y uint16
}

// TupleS is an auto generated low-level Go binding around an user-defined struct.
type TupleS struct {
	A *big.Int
	B []*big.Int
	C []TupleT
}

// TupleT is an auto generated low-level Go binding around an user-defined struct.
type TupleT struct {
	X *big.Int
	Y *big.Int
}

// TupleABI is the input ABI used to generate the binding from.
const TupleABI = "[{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"indexed\":false,\"internalType\":\"structTuple.S\",\"name\":\"a\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"indexed\":false,\"internalType\":\"structTuple.T[2][]\",\"name\":\"b\",\"type\":\"tuple[2][]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"indexed\":false,\"internalType\":\"structTuple.T[][2]\",\"name\":\"c\",\"type\":\"tuple[][2]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"indexed\":false,\"internalType\":\"structTuple.S[]\",\"name\":\"d\",\"type\":\"tuple[]\"},{\"indexed\":false,\"internalType\":\"uint256[]\",\"name\":\"e\",\"type\":\"uint256[]\"}],\"name\":\"TupleEvent\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"components\":[{\"internalType\":\"uint8\",\"name\":\"x\",\"type\":\"uint8\"},{\"internalType\":\"uint8\",\"name\":\"y\",\"type\":\"uint8\"}],\"indexed\":false,\"internalType\":\"structTuple.P[]\",\"name\":\"\",\"type\":\"tuple[]\"}],\"name\":\"TupleEvent2\",\"type\":\"event\"},{\"constant\":true,\"inputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"internalType\":\"structTuple.S\",\"name\":\"a\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[2][]\",\"name\":\"b\",\"type\":\"tuple[2][]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[][2]\",\"name\":\"c\",\"type\":\"tuple[][2]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"internalType\":\"structTuple.S[]\",\"name\":\"d\",\"type\":\"tuple[]\"},{\"internalType\":\"uint256[]\",\"name\":\"e\",\"type\":\"uint256[]\"}],\"name\":\"func1\",\"outputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"internalType\":\"structTuple.S\",\"name\":\"\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[2][]\",\"name\":\"\",\"type\":\"tuple[2][]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[][2]\",\"name\":\"\",\"type\":\"tuple[][2]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"internalType\":\"structTuple.S[]\",\"name\":\"\",\"type\":\"tuple[]\"},{\"internalType\":\"uint256[]\",\"name\":\"\",\"type\":\"uint256[]\"}],\"payable\":false,\"stateMutability\":\"pure\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"internalType\":\"structTuple.S\",\"name\":\"a\",\"type\":\"tuple\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[2][]\",\"name\":\"b\",\"type\":\"tuple[2][]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[][2]\",\"name\":\"c\",\"type\":\"tuple[][2]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"b\",\"type\":\"uint256[]\"},{\"components\":[{\"internalType\":\"uint256\",\"name\":\"x\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"y\",\"type\":\"uint256\"}],\"internalType\":\"structTuple.T[]\",\"name\":\"c\",\"type\":\"tuple[]\"}],\"internalType\":\"structTuple.S[]\",\"name\":\"d\",\"type\":\"tuple[]\"},{\"internalType\":\"uint256[]\",\"name\":\"e\",\"type\":\"uint256[]\"}],\"name\":\"func2\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"components\":[{\"internalType\":\"uint16\",\"name\":\"x\",\"type\":\"uint16\"},{\"internalType\":\"uint16\",\"name\":\"y\",\"type\":\"uint16\"}],\"internalType\":\"structTuple.Q[]\",\"name\":\"\",\"type\":\"tuple[]\"}],\"name\":\"func3\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"pure\",\"type\":\"function\"}]"

// TupleBin is the compiled bytecode used for deploying new contracts.
var TupleBin = "0x60806040523480156100115760006000fd5b50610017565b6110b2806100266000396000f3fe60806040523480156100115760006000fd5b50600436106100465760003560e01c8063443c79b41461004c578063d0062cdd14610080578063e4d9a43b1461009c57610046565b60006000fd5b610066600480360361006191908101906107b8565b6100b8565b604051610077959493929190610ccb565b60405180910390f35b61009a600480360361009591908101906107b8565b6100ef565b005b6100b660048036036100b19190810190610775565b610136565b005b6100c061013a565b60606100ca61015e565b606060608989898989945094509450945094506100e2565b9550955095509550959050565b7f18d6e66efa53739ca6d13626f35ebc700b31cced3eddb50c70bbe9c082c6cd008585858585604051610126959493929190610ccb565b60405180910390a15b5050505050565b5b50565b60405180606001604052806000815260200160608152602001606081526020015090565b60405180604001604052806002905b606081526020019060019003908161016d57905050905661106e565b600082601f830112151561019d5760006000fd5b81356101b06101ab82610d6f565b610d41565b915081818352602084019350602081019050838560808402820111156101d65760006000fd5b60005b8381101561020757816101ec888261037a565b8452602084019350608083019250505b6001810190506101d9565b5050505092915050565b600082601f83011215156102255760006000fd5b600261023861023382610d98565b610d41565b9150818360005b83811015610270578135860161025588826103f3565b8452602084019350602083019250505b60018101905061023f565b5050505092915050565b600082601f830112151561028e5760006000fd5b81356102a161029c82610dbb565b610d41565b915081818352602084019350602081019050838560408402820111156102c75760006000fd5b60005b838110156102f857816102dd888261058b565b8452602084019350604083019250505b6001810190506102ca565b5050505092915050565b600082601f83011215156103165760006000fd5b813561032961032482610de4565b610d41565b9150818183526020840193506020810190508360005b83811015610370578135860161035588826105d8565b8452602084019350602083019250505b60018101905061033f565b5050505092915050565b600082601f830112151561038e5760006000fd5b60026103a161039c82610e0d565b610d41565b915081838560408402820111156103b85760006000fd5b60005b838110156103e957816103ce88826106fe565b8452602084019350604083019250505b6001810190506103bb565b5050505092915050565b600082601f83011215156104075760006000fd5b813561041a61041582610e30565b610d41565b915081818352602084019350602081019050838560408402820111156104405760006000fd5b60005b83811015610471578161045688826106fe565b8452602084019350604083019250505b600181019050610443565b5050505092915050565b600082601f830112151561048f5760006000fd5b81356104a261049d82610e59565b610d41565b915081818352602084019350602081019050838560208402820111156104c85760006000fd5b60005b838110156104f957816104de8882610760565b8452602084019350602083019250505b6001810190506104cb565b5050505092915050565b600082601f83011215156105175760006000fd5b813561052a61052582610e82565b610d41565b915081818352602084019350602081019050838560208402820111156105505760006000fd5b60005b8381101561058157816105668882610760565b8452602084019350602083019250505b600181019050610553565b5050505092915050565b60006040828403121561059e5760006000fd5b6105a86040610d41565b905060006105b88482850161074b565b60008301525060206105cc8482850161074b565b60208301525092915050565b6000606082840312156105eb5760006000fd5b6105f56060610d41565b9050600061060584828501610760565b600083015250602082013567ffffffffffffffff8111156106265760006000fd5b6106328482850161047b565b602083015250604082013567ffffffffffffffff8111156106535760006000fd5b61065f848285016103f3565b60408301525092915050565b60006060828403121561067e5760006000fd5b6106886060610d41565b9050600061069884828501610760565b600083015250602082013567ffffffffffffffff8111156106b95760006000fd5b6106c58482850161047b565b602083015250604082013567ffffffffffffffff8111156106e65760006000fd5b6106f2848285016103f3565b60408301525092915050565b6000604082840312156107115760006000fd5b61071b6040610d41565b9050600061072b84828501610760565b600083015250602061073f84828501610760565b60208301525092915050565b60008135905061075a8161103a565b92915050565b60008135905061076f81611054565b92915050565b6000602082840312156107885760006000fd5b600082013567ffffffffffffffff8111156107a35760006000fd5b6107af8482850161027a565b91505092915050565b6000600060006000600060a086880312156107d35760006000fd5b600086013567ffffffffffffffff8111156107ee5760006000fd5b6107fa8882890161066b565b955050602086013567ffffffffffffffff8111156108185760006000fd5b61082488828901610189565b945050604086013567ffffffffffffffff8111156108425760006000fd5b61084e88828901610211565b935050606086013567ffffffffffffffff81111561086c5760006000fd5b61087888828901610302565b925050608086013567ffffffffffffffff8111156108965760006000fd5b6108a288828901610503565b9150509295509295909350565b60006108bb8383610a6a565b60808301905092915050565b60006108d38383610ac2565b905092915050565b60006108e78383610c36565b905092915050565b60006108fb8383610c8d565b60408301905092915050565b60006109138383610cbc565b60208301905092915050565b600061092a82610f0f565b6109348185610fb7565b935061093f83610eab565b8060005b8381101561097157815161095788826108af565b975061096283610f5c565b9250505b600181019050610943565b5085935050505092915050565b600061098982610f1a565b6109938185610fc8565b9350836020820285016109a585610ebb565b8060005b858110156109e257848403895281516109c285826108c7565b94506109cd83610f69565b925060208a019950505b6001810190506109a9565b50829750879550505050505092915050565b60006109ff82610f25565b610a098185610fd3565b935083602082028501610a1b85610ec5565b8060005b85811015610a585784840389528151610a3885826108db565b9450610a4383610f76565b925060208a019950505b600181019050610a1f565b50829750879550505050505092915050565b610a7381610f30565b610a7d8184610fe4565b9250610a8882610ed5565b8060005b83811015610aba578151610aa087826108ef565b9650610aab83610f83565b9250505b600181019050610a8c565b505050505050565b6000610acd82610f3b565b610ad78185610fef565b9350610ae283610edf565b8060005b83811015610b14578151610afa88826108ef565b9750610b0583610f90565b9250505b600181019050610ae6565b5085935050505092915050565b6000610b2c82610f51565b610b368185611011565b9350610b4183610eff565b8060005b83811015610b73578151610b598882610907565b9750610b6483610faa565b9250505b600181019050610b45565b5085935050505092915050565b6000610b8b82610f46565b610b958185611000565b9350610ba083610eef565b8060005b83811015610bd2578151610bb88882610907565b9750610bc383610f9d565b9250505b600181019050610ba4565b5085935050505092915050565b6000606083016000830151610bf76000860182610cbc565b5060208301518482036020860152610c0f8282610b80565b91505060408301518482036040860152610c298282610ac2565b9150508091505092915050565b6000606083016000830151610c4e6000860182610cbc565b5060208301518482036020860152610c668282610b80565b91505060408301518482036040860152610c808282610ac2565b9150508091505092915050565b604082016000820151610ca36000850182610cbc565b506020820151610cb66020850182610cbc565b50505050565b610cc581611030565b82525050565b600060a0820190508181036000830152610ce58188610bdf565b90508181036020830152610cf9818761091f565b90508181036040830152610d0d818661097e565b90508181036060830152610d2181856109f4565b90508181036080830152610d358184610b21565b90509695505050505050565b6000604051905081810181811067ffffffffffffffff82111715610d655760006000fd5b8060405250919050565b600067ffffffffffffffff821115610d875760006000fd5b602082029050602081019050919050565b600067ffffffffffffffff821115610db05760006000fd5b602082029050919050565b600067ffffffffffffffff821115610dd35760006000fd5b602082029050602081019050919050565b600067ffffffffffffffff821115610dfc5760006000fd5b602082029050602081019050919050565b600067ffffffffffffffff821115610e255760006000fd5b602082029050919050565b600067ffffffffffffffff821115610e485760006000fd5b602082029050602081019050919050565b600067ffffffffffffffff821115610e715760006000fd5b602082029050602081019050919050565b600067ffffffffffffffff821115610e9a5760006000fd5b602082029050602081019050919050565b6000819050602082019050919050565b6000819050919050565b6000819050602082019050919050565b6000819050919050565b6000819050602082019050919050565b6000819050602082019050919050565b6000819050602082019050919050565b600081519050919050565b600060029050919050565b600081519050919050565b600060029050919050565b600081519050919050565b600081519050919050565b600081519050919050565b6000602082019050919050565b6000602082019050919050565b6000602082019050919050565b6000602082019050919050565b6000602082019050919050565b6000602082019050919050565b6000602082019050919050565b600082825260208201905092915050565b600081905092915050565b600082825260208201905092915050565b600081905092915050565b600082825260208201905092915050565b600082825260208201905092915050565b600082825260208201905092915050565b600061ffff82169050919050565b6000819050919050565b61104381611022565b811415156110515760006000fd5b50565b61105d81611030565b8114151561106b5760006000fd5b50565bfea365627a7a72315820d78c6ba7ee332581e6c4d9daa5fc07941841230f7ce49edf6e05b1b63853e8746c6578706572696d656e74616cf564736f6c634300050c0040"

// DeployTuple deploys a new Ethereum contract, binding an instance of Tuple to it.
func DeployTuple(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Tuple, error) {
	parsed, err := abi.JSON(strings.NewReader(TupleABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(TupleBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Tuple{TupleCaller: TupleCaller{contract: contract}, TupleTransactor: TupleTransactor{contract: contract}, TupleFilterer: TupleFilterer{contract: contract}}, nil
}

// Tuple is an auto generated Go binding around an Ethereum contract.
type Tuple struct {
	TupleCaller     // Read-only binding to the contract
	TupleTransactor // Write-only binding to the contract
	TupleFilterer   // Log filterer for contract events
}

// TupleCaller is an auto generated read-only Go binding around an Ethereum contract.
type TupleCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TupleTransactor is an auto generated write-only Go binding around an Ethereum contract.
type TupleTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TupleFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type TupleFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TupleSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type TupleSession struct {
	Contract     *Tuple            // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TupleCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type TupleCallerSession struct {
	Contract *TupleCaller  // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// TupleTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type TupleTransactorSession struct {
	Contract     *TupleTransactor  // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TupleRaw is an auto generated low-level Go binding around an Ethereum contract.
type TupleRaw struct {
	Contract *Tuple // Generic contract binding to access the raw methods on
}

// TupleCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type TupleCallerRaw struct {
	Contract *TupleCaller // Generic read-only contract binding to access the raw methods on
}

// TupleTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type TupleTransactorRaw struct {
	Contract *TupleTransactor // Generic write-only contract binding to access the raw methods on
}

// NewTuple creates a new instance of Tuple, bound to a specific deployed contract.
func NewTuple(address common.Address, backend bind.ContractBackend) (*Tuple, error) {
	contract, err := bindTuple(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Tuple{TupleCaller: TupleCaller{contract: contract}, TupleTransactor: TupleTransactor{contract: contract}, TupleFilterer: TupleFilterer{contract: contract}}, nil
}

// NewTupleCaller creates a new read-only instance of Tuple, bound to a specific deployed contract.
func NewTupleCaller(address common.Address, caller bind.ContractCaller) (*TupleCaller, error) {
	contract, err := bindTuple(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &TupleCaller{contract: contract}, nil
}

// NewTupleTransactor creates a new write-only instance of Tuple, bound to a specific deployed contract.
func NewTupleTransactor(address common.Address, transactor bind.ContractTransactor) (*TupleTransactor, error) {
	contract, err := bindTuple(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &TupleTransactor{contract: contract}, nil
}

// NewTupleFilterer creates a new log filterer instance of Tuple, bound to a specific deployed contract.
func NewTupleFilterer(address common.Address, filterer bind.ContractFilterer) (*TupleFilterer, error) {
	contract, err := bindTuple(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &TupleFilterer{contract: contract}, nil
}

// bindTuple binds a generic wrapper to an already deployed contract.
func bindTuple(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(TupleABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Tuple *TupleRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Tuple.Contract.TupleCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Tuple *TupleRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Tuple.Contract.TupleTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Tuple *TupleRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Tuple.Contract.TupleTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Tuple *TupleCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Tuple.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Tuple *TupleTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Tuple.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Tuple *TupleTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Tuple.Contract.contract.Transact(opts, method, params...)
}

// Func1 is a free data retrieval call binding the contract method 0x443c79b4.
//
// Solidity: function func1((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e) pure returns((uint256,uint256[],(uint256,uint256)[]), (uint256,uint256)[2][], (uint256,uint256)[][2], (uint256,uint256[],(uint256,uint256)[])[], uint256[])
func (_Tuple *TupleCaller) Func1(opts *bind.CallOpts, a TupleS, b [][2]TupleT, c [2][]TupleT, d []TupleS, e []*big.Int) (TupleS, [][2]TupleT, [2][]TupleT, []TupleS, []*big.Int, error) {
	var out []interface{}
	err := _Tuple.contract.Call(opts, &out, "func1", a, b, c, d, e)

	if err != nil {
		return *new(TupleS), *new([][2]TupleT), *new([2][]TupleT), *new([]TupleS), *new([]*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(TupleS)).(*TupleS)
	out1 := *abi.ConvertType(out[1], new([][2]TupleT)).(*[][2]TupleT)
	out2 := *abi.ConvertType(out[2], new([2][]TupleT)).(*[2][]TupleT)
	out3 := *abi.ConvertType(out[3], new([]TupleS)).(*[]TupleS)
	out4 := *abi.ConvertType(out[4], new([]*big.Int)).(*[]*big.Int)

	return out0, out1, out2, out3, out4, err

}

// Func1 is a free data retrieval call binding the contract method 0x443c79b4.
//
// Solidity: function func1((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e) pure returns((uint256,uint256[],(uint256,uint256)[]), (uint256,uint256)[2][], (uint256,uint256)[][2], (uint256,uint256[],(uint256,uint256)[])[], uint256[])
func (_Tuple *TupleSession) Func1(a TupleS, b [][2]TupleT, c [2][]TupleT, d []TupleS, e []*big.Int) (TupleS, [][2]TupleT, [2][]TupleT, []TupleS, []*big.Int, error) {
	return _Tuple.Contract.Func1(&_Tuple.CallOpts, a, b, c, d, e)
}

// Func1 is a free data retrieval call binding the contract method 0x443c79b4.
//
// Solidity: function func1((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e) pure returns((uint256,uint256[],(uint256,uint256)[]), (uint256,uint256)[2][], (uint256,uint256)[][2], (uint256,uint256[],(uint256,uint256)[])[], uint256[])
func (_Tuple *TupleCallerSession) Func1(a TupleS, b [][2]TupleT, c [2][]TupleT, d []TupleS, e []*big.Int) (TupleS, [][2]TupleT, [2][]TupleT, []TupleS, []*big.Int, error) {
	return _Tuple.Contract.Func1(&_Tuple.CallOpts, a, b, c, d, e)
}

// Func3 is a free data retrieval call binding the contract method 0xe4d9a43b.
//
// Solidity: function func3((uint16,uint16)[] ) pure returns()
func (_Tuple *TupleCaller) Func3(opts *bind.CallOpts, arg0 []TupleQ) error {
	var out []interface{}
	err := _Tuple.contract.Call(opts, &out, "func3", arg0)

	if err != nil {
		return err
	}

	return err

}

// Func3 is a free data retrieval call binding the contract method 0xe4d9a43b.
//
// Solidity: function func3((uint16,uint16)[] ) pure returns()
func (_Tuple *TupleSession) Func3(arg0 []TupleQ) error {
	return _Tuple.Contract.Func3(&_Tuple.CallOpts, arg0)
}

// Func3 is a free data retrieval call binding the contract method 0xe4d9a43b.
//
// Solidity: function func3((uint16,uint16)[] ) pure returns()
func (_Tuple *TupleCallerSession) Func3(arg0 []TupleQ) error {
	return _Tuple.Contract.Func3(&_Tuple.CallOpts, arg0)
}

// Func2 is a paid mutator transaction binding the contract method 0xd0062cdd.
//
// Solidity: function func2((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e) returns()
func (_Tuple *TupleTransactor) Func2(opts *bind.TransactOpts, a TupleS, b [][2]TupleT, c [2][]TupleT, d []TupleS, e []*big.Int) (*types.Transaction, error) {
	return _Tuple.contract.Transact(opts, "func2", a, b, c, d, e)
}

// Func2 is a paid mutator transaction binding the contract method 0xd0062cdd.
//
// Solidity: function func2((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e) returns()
func (_Tuple *TupleSession) Func2(a TupleS, b [][2]TupleT, c [2][]TupleT, d []TupleS, e []*big.Int) (*types.Transaction, error) {
	return _Tuple.Contract.Func2(&_Tuple.TransactOpts, a, b, c, d, e)
}

// Func2 is a paid mutator transaction binding the contract method 0xd0062cdd.
//
// Solidity: function func2((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e) returns()
func (_Tuple *TupleTransactorSession) Func2(a TupleS, b [][2]TupleT, c [2][]TupleT, d []TupleS, e []*big.Int) (*types.Transaction, error) {
	return _Tuple.Contract.Func2(&_Tuple.TransactOpts, a, b, c, d, e)
}

// TupleTupleEventIterator is returned from FilterTupleEvent and is used to iterate over the raw logs and unpacked data for TupleEvent events raised by the Tuple contract.
type TupleTupleEventIterator struct {
	Event *TupleTupleEvent // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *TupleTupleEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TupleTupleEvent)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(TupleTupleEvent)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *TupleTupleEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TupleTupleEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TupleTupleEvent represents a TupleEvent event raised by the Tuple contract.
type TupleTupleEvent struct {
	A   TupleS
	B   [][2]TupleT
	C   [2][]TupleT
	D   []TupleS
	E   []*big.Int
	Raw types.Log // Blockchain specific contextual infos
}

// FilterTupleEvent is a free log retrieval operation binding the contract event 0x18d6e66efa53739ca6d13626f35ebc700b31cced3eddb50c70bbe9c082c6cd00.
//
// Solidity: event TupleEvent((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e)
func (_Tuple *TupleFilterer) FilterTupleEvent(opts *bind.FilterOpts) (*TupleTupleEventIterator, error) {

	logs, sub, err := _Tuple.contract.FilterLogs(opts, "TupleEvent")
	if err != nil {
		return nil, err
	}
	return &TupleTupleEventIterator{contract: _Tuple.contract, event: "TupleEvent", logs: logs, sub: sub}, nil
}

// WatchTupleEvent is a free log subscription operation binding the contract event 0x18d6e66efa53739ca6d13626f35ebc700b31cced3eddb50c70bbe9c082c6cd00.
//
// Solidity: event TupleEvent((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e)
func (_Tuple *TupleFilterer) WatchTupleEvent(opts *bind.WatchOpts, sink chan<- *TupleTupleEvent) (event.Subscription, error) {

	logs, sub, err := _Tuple.contract.WatchLogs(opts, "TupleEvent")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TupleTupleEvent)
				if err := _Tuple.contract.UnpackLog(event, "TupleEvent", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseTupleEvent is a log parse operation binding the contract event 0x18d6e66efa53739ca6d13626f35ebc700b31cced3eddb50c70bbe9c082c6cd00.
//
// Solidity: event TupleEvent((uint256,uint256[],(uint256,uint256)[]) a, (uint256,uint256)[2][] b, (uint256,uint256)[][2] c, (uint256,uint256[],(uint256,uint256)[])[] d, uint256[] e)
func (_Tuple *TupleFilterer) ParseTupleEvent(log types.Log) (*TupleTupleEvent, error) {
	event := new(TupleTupleEvent)
	if err := _Tuple.contract.UnpackLog(event, "TupleEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}

// TupleTupleEvent2Iterator is returned from FilterTupleEvent2 and is used to iterate over the raw logs and unpacked data for TupleEvent2 events raised by the Tuple contract.
type TupleTupleEvent2Iterator struct {
	Event *TupleTupleEvent2 // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *TupleTupleEvent2Iterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TupleTupleEvent2)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(TupleTupleEvent2)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *TupleTupleEvent2Iterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TupleTupleEvent2Iterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TupleTupleEvent2 represents a TupleEvent2 event raised by the Tuple contract.
type TupleTupleEvent2 struct {
	Arg0 []TupleP
	Raw  types.Log // Blockchain specific contextual infos
}

// FilterTupleEvent2 is a free log retrieval operation binding the contract event 0x88833a3455c364501b6a838291e9240d528d80babaa9916b2cba73f4f95b8d50.
//
// Solidity: event TupleEvent2((uint8,uint8)[] arg0)
func (_Tuple *TupleFilterer) FilterTupleEvent2(opts *bind.FilterOpts) (*TupleTupleEvent2Iterator, error) {

	logs, sub, err := _Tuple.contract.FilterLogs(opts, "TupleEvent2")
	if err != nil {
		return nil, err
	}
	return &TupleTupleEvent2Iterator{contract: _Tuple.contract, event: "TupleEvent2", logs: logs, sub: sub}, nil
}

// WatchTupleEvent2 is a free log subscription operation binding the contract event 0x88833a3455c364501b6a838291e9240d528d80babaa9916b2cba73f4f95b8d50.
//
// Solidity: event TupleEvent2((uint8,uint8)[] arg0)
func (_Tuple *TupleFilterer) WatchTupleEvent2(opts *bind.WatchOpts, sink chan<- *TupleTupleEvent2) (event.Subscription, error) {

	logs, sub, err := _Tuple.contract.WatchLogs(opts, "TupleEvent2")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TupleTupleEvent2)
				if err := _Tuple.contract.UnpackLog(event, "TupleEvent2", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseTupleEvent2 is a log parse operation binding the contract event 0x88833a3455c364501b6a838291e9240d528d80babaa9916b2cba73f4f95b8d50.
//
// Solidity: event TupleEvent2((uint8,uint8)[] arg0)
func (_Tuple *TupleFilterer) ParseTupleEvent2(log types.Log) (*TupleTupleEvent2, error) {
	event := new(TupleTupleEvent2)
	if err := _Tuple.contract.UnpackLog(event, "TupleEvent2", log); err != nil {
		return nil, err
	}
	return event, nil
}
