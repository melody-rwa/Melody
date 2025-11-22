// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

/*
FractionalVaultV3 (singleton vesting vault version, simplified)

Key changes from previous version:
- Removed nonRedeemable mapping.
- redeem() now simply forbids platformFeeMultiSig from calling it (platform 17% locked).
- VestingVault uses the same redeem() to redeem unlocked amounts (redeemed tokens are transferred to msg.sender).
- rewardDebt for newly assigned userLiquidity is initialized to current accPer to avoid unexpected pending amounts.
*/

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/ReentrancyGuardUpgradeable.sol";
import "@openzeppelin/contracts/token/ERC721/IERC721Receiver.sol";

interface INonfungiblePositionManager {
    struct CollectParams {
        uint256 tokenId;
        address recipient;
        uint128 amount0Max;
        uint128 amount1Max;
    }
    struct DecreaseLiquidityParams {
        uint256 tokenId;
        uint128 liquidity;
        uint256 amount0Min;
        uint256 amount1Min;
        uint256 deadline;
    }
    struct MintParams {
        address token0;
        address token1;
        uint24 fee;
        int24 tickLower;
        int24 tickUpper;
        uint256 amount0Desired;
        uint256 amount1Desired;
        uint256 amount0Min;
        uint256 amount1Min;
        address recipient;
        uint256 deadline;
    }

    function mint(MintParams calldata params)
        external
        payable
        returns (uint256 tokenId, uint128 liquidity, uint256 amount0, uint256 amount1);
    function collect(CollectParams calldata params)
        external
        returns (uint256 amount0, uint256 amount1);

    function decreaseLiquidity(DecreaseLiquidityParams calldata params)
        external
        returns (uint256 amount0, uint256 amount1);

    function positions(uint256 tokenId)
        external
        view
        returns (
            uint96 nonce,
            address operator,
            address token0,
            address token1,
            uint24 fee,
            int24 tickLower,
            int24 tickUpper,
            uint128 liquidity,
            uint256 feeGrowthInside0LastX128,
            uint256 feeGrowthInside1LastX128,
            uint128 tokensOwed0,
            uint128 tokensOwed1
        );

    function safeTransferFrom(address from, address to, uint256 tokenId) external;
}
interface IVestingVault{
    function addVesting(
        uint256 songId,
        address artist,
        uint128 amount,   
        uint256 start,
        uint256 duration
    ) external;
}
contract FractionalVaultV3 is Initializable, AccessControlUpgradeable, UUPSUpgradeable, ReentrancyGuardUpgradeable,IERC721Receiver {
    using SafeERC20 for IERC20;

    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    bytes32 public constant MANAGER_ROLE  = keccak256("MANAGER_ROLE");
    bytes32 public constant ADMIN_ROLE    = keccak256("ADMIN_ROLE");

    uint256 private constant SCALER = 1e18;

    struct DistributionInfo {
        uint256 tokenId;
        address token0;
        address token1;
        uint128 liquidity;    // total liquidity units for this V3 position
        address vestingVault; // singleton vesting vault address (holds artistShare)
        address artist;
        bool distributed;
        uint24 fee;
        int24 tickLower;
        int24 tickUpper;
    }

    // storage
    mapping(uint256 => DistributionInfo) public distributions; // songId -> info
    uint256[] private allSongIds;
    mapping(address => uint256[]) private artistToSongs;

    // accounting (scaled)
    mapping(uint256 => uint256) public accToken0PerLiquidity; // songId => accumulated token0 per liquidity (scaled)
    mapping(uint256 => uint256) public accToken1PerLiquidity; // songId => accumulated token1 per liquidity (scaled)

    // user data per song
    mapping(uint256 => mapping(address => uint128)) public userLiquidity; // songId => user => liquidity
    mapping(uint256 => mapping(address => uint256)) public rewardDebt0;   // songId => user => debt
    mapping(uint256 => mapping(address => uint256)) public rewardDebt1;   // songId => user => debt

    // refs
    INonfungiblePositionManager public positionManager;
    address public platformMultiSig;    // 3% treated as normal user (can redeem)
    address public platformFeeMultiSig;    // 17% locked LP, cannot redeem (but can claim fees)
    address public lpManager;           // only this may call finalizeDistribution
    address public vestingVault;        // singleton vesting vault address (manages artists' unlocking)
    address public admin;

    // events
    event FinalizedDistribution(uint256 indexed songId, uint256 tokenId, uint128 liquidity, address vestingVault);
    event Claimed(address indexed user, uint256 indexed songId, uint256 amount0, uint256 amount1,address token0,address token1);
    event Redeemed(address indexed user, uint256 indexed songId, uint128 liqRemoved, uint256 amount0, uint256 amount1,address token0,address token1);
    event ReAddLiquidityToUser(address indexed user, uint256 indexed songId, uint256 tokenId, uint128 liquidity, uint256 used0, uint256 used1);

    // initializer
    function initialize(
        address _positionManager,
        address _admin,
        address _platformFeeMultiSig,
        address _platformMultiSig,
        address _lpManager,
        address _vestingVault,
        address upgrader
    ) public initializer {
        require(_positionManager != address(0) && _admin != address(0) && _lpManager != address(0) && upgrader != address(0), "zero addr");
        __AccessControl_init();
        __UUPSUpgradeable_init();
        __ReentrancyGuard_init();

        positionManager = INonfungiblePositionManager(_positionManager);
        admin = _admin;
        platformFeeMultiSig = _platformFeeMultiSig;
        platformMultiSig = _platformMultiSig;
        lpManager = _lpManager;
        vestingVault = _vestingVault;

        _grantRole(DEFAULT_ADMIN_ROLE, _admin);
        _grantRole(UPGRADER_ROLE, upgrader);
        _grantRole(ADMIN_ROLE, upgrader);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    // admin setters
    function setPositionManager(address _pm) external onlyRole(ADMIN_ROLE) {
        require(_pm != address(0), "pm=0");
        positionManager = INonfungiblePositionManager(_pm);
    }
    function setplatformFeeMultiSig(address pv) external onlyRole(ADMIN_ROLE) {
        platformFeeMultiSig = pv;
    }
    function setPlatformMultiSig(address msig) external onlyRole(ADMIN_ROLE) {
        platformMultiSig = msig;
    }
    function setLpManager(address mgr) external onlyRole(ADMIN_ROLE) {
        lpManager = mgr;
    }
    function setVestingVault(address vv) external onlyRole(ADMIN_ROLE) {
        vestingVault = vv;
    }

    // finalizeDistribution - called by lpManager after the V3 position (NFT) is transferred to this contract
    function finalizeDistribution(
        uint256 songId,
        uint256 tokenId,
        address token0,
        address token1,
        uint128 liquidity,
        address[] calldata userAddrs,
        uint128[] calldata userLiqAmounts,
        address artist,
        uint24 fee,
        int24 tickLower,
        int24 tickUpper
    ) external {
        require(msg.sender == lpManager, "only lpManager");
        require(!distributions[songId].distributed, "already distributed");
        require(tokenId != 0 && liquidity > 0, "bad params");
        require(artist != address(0), "artist=0");
        require(userAddrs.length == userLiqAmounts.length, "len mismatch");

        // partition
        uint128 usersShare = uint128((uint256(liquidity) * 50) / 100);
        uint128 artistShare = uint128((uint256(liquidity) * 30) / 100);
        uint128 platform3Liq = uint128((uint256(liquidity) * 3) / 100);
        uint128 platform17Liq = uint128(uint256(liquidity) - usersShare - artistShare - platform3Liq);

        // validate user sums and store userLiquidity
        uint128 sumUsers = 0;
        // Get current accs to initialize rewardDebt to current level
        uint256 acc0 = accToken0PerLiquidity[songId];
        uint256 acc1 = accToken1PerLiquidity[songId];

        for (uint256 i = 0; i < userLiqAmounts.length; i++) {
            address u = userAddrs[i];
            uint128 lq = userLiqAmounts[i];
            require(u != address(0), "user=0");
            sumUsers += lq;
            userLiquidity[songId][u] = lq;
            // initialize rewardDebt to current accumulated (avoid immediate pending)
            rewardDebt0[songId][u] = (uint256(lq) * acc0) / SCALER;
            rewardDebt1[songId][u] = (uint256(lq) * acc1) / SCALER;
        }
        require(sumUsers == usersShare, "user share mismatch");

        // platform 3% -> treat as normal user (multiSig)
        if (platformMultiSig != address(0) && platform3Liq > 0) {
            userLiquidity[songId][platformMultiSig] += platform3Liq;
            // update debt baseline
            rewardDebt0[songId][platformMultiSig] = (uint256(userLiquidity[songId][platformMultiSig]) * acc0) / SCALER;
            rewardDebt1[songId][platformMultiSig] = (uint256(userLiquidity[songId][platformMultiSig]) * acc1) / SCALER;
        }

        // platform 17% -> platformFeeMultiSig (locked LP, cannot redeem)
        if (platformFeeMultiSig != address(0) && platform17Liq > 0) {
            userLiquidity[songId][platformFeeMultiSig] += platform17Liq;
            rewardDebt0[songId][platformFeeMultiSig] = (uint256(userLiquidity[songId][platformFeeMultiSig]) * acc0) / SCALER;
            rewardDebt1[songId][platformFeeMultiSig] = (uint256(userLiquidity[songId][platformFeeMultiSig]) * acc1) / SCALER;
        }

        // artist 30% -> vestingVault singleton (must be set)
        require(vestingVault != address(0), "vestingVault not set");
        if (artistShare > 0) {
            userLiquidity[songId][vestingVault] += artistShare;
            rewardDebt0[songId][vestingVault] = (uint256(userLiquidity[songId][vestingVault]) * acc0) / SCALER;
            rewardDebt1[songId][vestingVault] = (uint256(userLiquidity[songId][vestingVault]) * acc1) / SCALER;
            IVestingVault(vestingVault).addVesting(songId, artist, artistShare, block.timestamp, 2 * 356 days);
        }

        // store distribution record
        distributions[songId] = DistributionInfo({
            tokenId: tokenId,
            token0: token0,
            token1: token1,
            liquidity: liquidity,
            vestingVault: vestingVault,
            artist: artist,
            distributed: true,
            fee:fee,
            tickLower:tickLower,
            tickUpper:tickUpper
        });

        allSongIds.push(songId);
        artistToSongs[artist].push(songId);

        emit FinalizedDistribution(songId, tokenId, liquidity, vestingVault);
    }

    // collect fees from V3 position to this contract and update accPerLiquidity
    function collectFees(uint256 songId) public {
        DistributionInfo storage d = distributions[songId];
        require(d.distributed, "not distributed");
        require(d.liquidity > 0, "no liquidity");

        (uint256 amount0, uint256 amount1) = positionManager.collect(
            INonfungiblePositionManager.CollectParams({
                tokenId: d.tokenId,
                recipient: address(this),
                amount0Max: type(uint128).max,
                amount1Max: type(uint128).max
            })
        );

        if (amount0 > 0) {
            accToken0PerLiquidity[songId] += (amount0 * SCALER) / d.liquidity;
        }
        if (amount1 > 0) {
            accToken1PerLiquidity[songId] += (amount1 * SCALER) / d.liquidity;
        }
    }

    // claim pending fees for caller
    function claim(uint256 songId) public nonReentrant returns(address token0, address token1, uint256 amount0, uint256 amount1){
        collectFees(songId);

        DistributionInfo storage d = distributions[songId];
        require(d.distributed, "not distributed");

        uint128 uLiq = userLiquidity[songId][msg.sender];
        require(uLiq > 0, "no share");

        uint256 acc0 = accToken0PerLiquidity[songId];
        uint256 acc1 = accToken1PerLiquidity[songId];

        uint256 totalAccForUser0 = (uint256(uLiq) * acc0) / SCALER;
        uint256 totalAccForUser1 = (uint256(uLiq) * acc1) / SCALER;

        uint256 debt0 = rewardDebt0[songId][msg.sender];
        uint256 debt1 = rewardDebt1[songId][msg.sender];

        if (totalAccForUser0 > debt0) {
            uint256 pay0 = totalAccForUser0 - debt0;
            rewardDebt0[songId][msg.sender] = totalAccForUser0;
            if (pay0 > 0) IERC20(d.token0).safeTransfer(msg.sender, pay0);
            amount0 = pay0;
        }
        if (totalAccForUser1 > debt1) {
            uint256 pay1 = totalAccForUser1 - debt1;
            rewardDebt1[songId][msg.sender] = totalAccForUser1;
            if (pay1 > 0) IERC20(d.token1).safeTransfer(msg.sender, pay1);
            amount1 = pay1;
        }
        token0 = d.token0;
        token1 =  d.token1;

        emit Claimed(msg.sender, songId, amount0,amount1,token0,token1);
    }

    // user redeem (self) - platformFeeMultiSig is forbidden to redeem
    // vestingVault can call this redeem for itself (it must ensure it only redeems unlocked amount)
// corrected redeem with collect step and safer ordering
    function redeem(
        uint256 songId,
        uint128 liquidityToRedeem,
        uint256 amount0Min,
        uint256 amount1Min
    )
    external
    nonReentrant
    returns (address token0, address token1, uint256 am0, uint256 am1)
{
    require(msg.sender != platformFeeMultiSig, "platform fee vault cannot redeem");
    DistributionInfo storage d = distributions[songId];
    require(d.distributed, "not distributed");

    uint128 uLiq = userLiquidity[songId][msg.sender];
    require(uLiq > 0, "zero liq");
    uint256 userType = 0;
    if(msg.sender == vestingVault) userType = 1;
    if(msg.sender == platformMultiSig) userType = 2;
    if(userType==0 || userType==2)liquidityToRedeem = uLiq;

    // 1) collect fees and update accPerLiquidity
    collectFees(songId);

    // 2) pay pending fees to caller (uses current uLiq)
    uint256 acc0 = accToken0PerLiquidity[songId];
    uint256 acc1 = accToken1PerLiquidity[songId];

    uint256 totalAccForUser0 = (uint256(uLiq) * acc0) / SCALER;
    uint256 totalAccForUser1 = (uint256(uLiq) * acc1) / SCALER;

    uint256 debt0 = rewardDebt0[songId][msg.sender];
    uint256 debt1 = rewardDebt1[songId][msg.sender];

    if (totalAccForUser0 > debt0) {
        uint256 pay0 = totalAccForUser0 - debt0;
        // update debt to totalAcc before transfer (prevents double-pay on reentrancy)
        rewardDebt0[songId][msg.sender] = totalAccForUser0;
        if (pay0 > 0) {
            IERC20(d.token0).safeTransfer(msg.sender, pay0);
            am0 += pay0;
        }
    }
    if (totalAccForUser1 > debt1) {
        uint256 pay1 = totalAccForUser1 - debt1;
        rewardDebt1[songId][msg.sender] = totalAccForUser1;
        if (pay1 > 0) {
            IERC20(d.token1).safeTransfer(msg.sender, pay1);
            am1 += pay1;
        }
    }

    // 3) check current position liquidity
    (, , , , , , , uint128 currentPosLiq, , , , ) = positionManager.positions(d.tokenId);
    require(currentPosLiq >= liquidityToRedeem, "pos low liq");

    // 4) decrease liquidity (burn) -> returns amounts but you still need to collect
    (uint256 amt0FromDecrease, uint256 amt1FromDecrease) = positionManager.decreaseLiquidity(
        INonfungiblePositionManager.DecreaseLiquidityParams({
        tokenId: d.tokenId,
        liquidity: liquidityToRedeem,
        amount0Min: amount0Min,
        amount1Min: amount1Min,
        deadline: block.timestamp
    })
    );

    // 5) collect the tokens actually into this contract (UNISWAP V3 style):
    //    signature commonly: collect((tokenId, recipient, amount0Max, amount1Max)) or collect(tokenId, recipient, amount0Max, amount1Max)
    //    Adjust this call to your positionManager interface if different.
    // Example periphery style:
    (uint256 amount0, uint256 amount1) = positionManager.collect(
        INonfungiblePositionManager.CollectParams({
            tokenId: d.tokenId,
            recipient: address(this),
            amount0Max: type(uint128).max,
            amount1Max: type(uint128).max
        })
    );

    // 6) transfer underlying tokens to caller (msg.sender)
    if(userType==0){
        IERC20(d.token0).approve(address(positionManager), 0);
        IERC20(d.token1).approve(address(positionManager), 0);
        IERC20(d.token0).approve(address(positionManager), amount0);
        IERC20(d.token1).approve(address(positionManager), amount1);

        // 构造 mint 参数
        INonfungiblePositionManager.MintParams memory params = INonfungiblePositionManager.MintParams({
            token0: d.token0,
            token1: d.token1,
            fee: d.fee,
            tickLower: d.tickLower,
            tickUpper: d.tickUpper,
            amount0Desired: amount0,
            amount1Desired: amount1,
            amount0Min: (amount0 * 99) / 100,
            amount1Min: (amount1 * 99) / 100,
            recipient: msg.sender,
            deadline: block.timestamp + 300
        });

        (uint256 newTokenId, uint128 reliquidity, uint256 used0, uint256 used1) = positionManager.mint(params);
        require(reliquidity > 0, "no reliquidity minted");

        
        if (used0 < amount0) {
            uint256 refund0 = amount0 - used0;
            am0 += refund0;
            IERC20(d.token0).safeTransfer(msg.sender, refund0);
        }
        if (used1 < amount1) {
            uint256 refund1 = amount1 - used1;
            am1 += refund1;
            IERC20(d.token1).safeTransfer(msg.sender, refund1);
        }

        emit ReAddLiquidityToUser(msg.sender, songId, newTokenId, reliquidity, used0, used1);
        //positionManager.safeTransferFrom(address(this), msg.sender, tokenId);
    }else{
        if (amount0 > 0) {
            IERC20(d.token0).safeTransfer(msg.sender, amount0);
            am0 += amount0;
        }
        if (amount1 > 0) {
            IERC20(d.token1).safeTransfer(msg.sender, amount1);
            am1 += amount1;
        }
    }
   

    // 7) update accounting AFTER successful transfers
    userLiquidity[songId][msg.sender] = uint128(uint256(uLiq) - uint256(liquidityToRedeem));
    d.liquidity = uint128(uint256(d.liquidity) - uint256(liquidityToRedeem));

    uint128 newULiq = userLiquidity[songId][msg.sender];
    rewardDebt0[songId][msg.sender] = (uint256(newULiq) * accToken0PerLiquidity[songId]) / SCALER;
    rewardDebt1[songId][msg.sender] = (uint256(newULiq) * accToken1PerLiquidity[songId]) / SCALER;

    token0 = d.token0;
    token1 = d.token1;

    emit Redeemed(msg.sender, songId, liquidityToRedeem, am0, am1,token0,token1);
}


    // view helpers
    function isDistributed(uint256 songId) external view returns (bool) {
        return distributions[songId].distributed;
    }

    function getDistribution(uint256 songId) external view returns (DistributionInfo memory) {
        return distributions[songId];
    }

    // estimate pending fees for a user (includes tokensOwed not yet collected)
    function getPendingForUser(uint256 songId, address user) external view returns (uint256 pending0, uint256 pending1) {
        DistributionInfo storage d = distributions[songId];
        if (!d.distributed) return (0,0);

        (, , , , , , , , , , uint128 owed0, uint128 owed1) = positionManager.positions(d.tokenId);

        uint256 acc0 = accToken0PerLiquidity[songId];
        uint256 acc1 = accToken1PerLiquidity[songId];
        if (d.liquidity > 0) {
            acc0 += (uint256(owed0) * SCALER) / uint256(d.liquidity);
            acc1 += (uint256(owed1) * SCALER) / uint256(d.liquidity);
        }

        uint128 uLiq = userLiquidity[songId][user];
        uint256 totalAccForUser0 = (uint256(uLiq) * acc0) / SCALER;
        uint256 totalAccForUser1 = (uint256(uLiq) * acc1) / SCALER;

        uint256 debt0 = rewardDebt0[songId][user];
        uint256 debt1 = rewardDebt1[songId][user];

        pending0 = totalAccForUser0 > debt0 ? totalAccForUser0 - debt0 : 0;
        pending1 = totalAccForUser1 > debt1 ? totalAccForUser1 - debt1 : 0;
    }

    // frontend convenience getters
    function getDistributions(uint256[] calldata songIds) external view returns (DistributionInfo[] memory infos) {
        infos = new DistributionInfo[](songIds.length);
        for (uint256 i = 0; i < songIds.length; i++) infos[i] = distributions[songIds[i]];
    }

    function getDistributionsByArtist(address artist) external view returns (DistributionInfo[] memory infos) {
        uint256[] storage ids = artistToSongs[artist];
        infos = new DistributionInfo[](ids.length);
        for (uint256 i = 0; i < ids.length; i++) infos[i] = distributions[ids[i]];
    }

    // admin emergency withdraw token (only admin)
    function adminWithdrawToken(address token, address to, uint256 amount) external onlyRole(ADMIN_ROLE) {
        IERC20(token).safeTransfer(to, amount);
    }
    function onERC721Received(
            address operator,
            address from,
            uint256 tokenId,
            bytes calldata data
        ) external pure override returns (bytes4) {
            return IERC721Receiver.onERC721Received.selector;
    }
    receive() external payable {}
}
