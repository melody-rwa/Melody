// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts/token/ERC721/IERC721Receiver.sol";
import "@openzeppelin/contracts/utils/math/Math.sol";
import "@uniswap/v3-core/contracts/libraries/FullMath.sol";


interface INonfungiblePositionManager {
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

    function safeTransferFrom(address from, address to, uint256 tokenId) external;
}
interface IUniswapV3Factory {
    /// @notice Returns the pool address for the given pair and fee. 
    /// Returns address(0) if it does not exist.
    function getPool(
        address tokenA, 
        address tokenB, 
        uint24 fee
    ) external view returns (address pool);

    /// @notice Creates a pool for the given pair and fee. 
    /// The caller must initialize it separately.
    function createPool(
        address tokenA, 
        address tokenB, 
        uint24 fee
    ) external returns (address pool);
}

/// @notice Uniswap V3 Pool interface 
interface IUniswapV3Pool {
    /// @notice Initializes the pool with a starting price
    /// @param sqrtPriceX96 the initial price sqrt(token1/token0) as a Q64.96
    function initialize(uint160 sqrtPriceX96) external;
    function tickSpacing() external view returns (int24);
}
interface IFractionalVaultV3 {
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
    ) external;
}

interface IMusicSeries {
    function getContributions(uint256 songId)
        external
        view
        returns (address[] memory users, uint256[] memory amounts);
}

contract LPManagerV3 is Initializable, AccessControlUpgradeable, UUPSUpgradeable,IERC721Receiver {
    using SafeERC20 for IERC20;

    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    bytes32 public constant ADMIN_ROLE    = keccak256("ADMIN_ROLE");
    bytes32 public constant SERIES_ROLE   = keccak256("SERIES_ROLE"); // registered MusicSeries

    INonfungiblePositionManager public positionManager;
    address public fractionalVault;
    address public musicFactory; // used to allow registerMusicSeries
    address public factory;

    event LiquidityAddedAndFinalized(uint256 indexed songId, uint256 tokenId, uint128 liquidity);


    function initialize(address _pm, address _fractionalVault, address admin,address _musicFactory,address _factory,address upgrader) public initializer {
        require(_pm != address(0) && _fractionalVault != address(0) && admin != address(0) && upgrader!=address(0), "zero addr");
        __AccessControl_init();
        __UUPSUpgradeable_init();

        positionManager = INonfungiblePositionManager(_pm);
        fractionalVault = _fractionalVault;
        musicFactory = _musicFactory;
        factory = _factory;

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(UPGRADER_ROLE, upgrader);
        _grantRole(ADMIN_ROLE, upgrader);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    // factory can register new MusicSeries
    function registerSeries(address series) external {
        require(msg.sender == musicFactory, "only factory");
        _grantRole(SERIES_ROLE, series);
    }

    /**
     * addLiquidityAndFinalizeV3
     * - Only callable by registered MusicSeries
     * - Pulls token0/token1 from caller, mints V3 LP NFT, then finalizes distribution in FractionalVaultV3
     */
    function addLiquidityAndFinalizeV3(
        uint256 songId,
        address token0,
        address token1,
        uint24 fee,
        int24 tickLower,
        int24 tickUpper,
        uint256 amount0Desired,
        uint256 amount1Desired,
        uint256 amount0Min,
        uint256 amount1Min,
        address artist
    ) external onlyRole(SERIES_ROLE) returns (uint256 tokenId, uint128 liquidity) {
        require(token0 != address(0) && token1 != address(0), "zero token");
        require(artist != address(0), "artist=0");
        if (token0 > token1) {
            (token0, token1) = (token1, token0);
            (amount0Desired, amount1Desired) = (amount1Desired, amount0Desired);
            (amount0Min, amount1Min) = (amount1Min, amount0Min);
        }

        require(amount0Desired > 0 && amount1Desired > 0, "zero amounts");
        // get contributions
        (address[] memory users, uint256[] memory contributions) = IMusicSeries(msg.sender).getContributions(songId);
        require(users.length == contributions.length && users.length > 0, "bad contribs");

        uint256 totalContrib = 0;
        for (uint256 i = 0; i < contributions.length; i++) {
            totalContrib += contributions[i];
        }
        require(totalContrib > 0, "total contrib zero");

        // transfer tokens from caller (MusicSeries)
        IERC20(token0).safeTransferFrom(msg.sender, address(this), amount0Desired);
        IERC20(token1).safeTransferFrom(msg.sender, address(this), amount1Desired);

        // approve position manager
        IERC20(token0).approve(address(positionManager), 0);
        IERC20(token0).approve(address(positionManager), amount0Desired);
        IERC20(token1).approve(address(positionManager), 0);
        IERC20(token1).approve(address(positionManager), amount1Desired);

        address pool = IUniswapV3Factory(factory).getPool(token0, token1, fee);
        require(pool == address(0), "Pool already exists");

        
        pool = IUniswapV3Factory(factory).createPool(token0, token1, fee);
       uint160 sqrtPriceX96 = encodeSqrtRatioX96(amount1Desired, amount0Desired);
        IUniswapV3Pool(pool).initialize(sqrtPriceX96);

        // mint position
        INonfungiblePositionManager.MintParams memory params = INonfungiblePositionManager.MintParams({
            token0: token0,
            token1: token1,
            fee: fee,
            tickLower: tickLower,
            tickUpper: tickUpper,
            amount0Desired: amount0Desired,
            amount1Desired: amount1Desired,
            amount0Min: 0,
            amount1Min: 0,
            recipient: address(this),
            deadline: block.timestamp+300
        });
        (tokenId,liquidity,,) = positionManager.mint(params);

        
        require(liquidity > 0, "no liquidity minted");
        {
        // compute user liquidity shares = 50% of total liquidity
        uint128 usersShare = uint128((uint256(liquidity) * 50) / 100);
        uint128[] memory userLiqAmounts = new uint128[](users.length);
        uint256 assigned = 0;
        for (uint256 i = 0; i < users.length; i++) {
            if (i < users.length - 1) {
                uint128 amt = uint128((usersShare * contributions[i]) / totalContrib);
                userLiqAmounts[i] = amt;
                assigned += amt;
            } else {
                userLiqAmounts[i] = usersShare - uint128(assigned);
            }
        }

        // transfer NFT to FractionalVault
        positionManager.safeTransferFrom(address(this), fractionalVault, tokenId);

        // finalize distribution in FractionalVaultV3
        IFractionalVaultV3(fractionalVault).finalizeDistribution(
            songId,
            tokenId,
            token0,
            token1,
            liquidity,
            users,
            userLiqAmounts,
            artist,
            fee,
            tickLower,
            tickUpper
        );
        }
        emit LiquidityAddedAndFinalized(songId, tokenId, liquidity);
        return (tokenId, liquidity);
    }

function encodeSqrtRatioX96(uint256 amount1, uint256 amount0) internal pure returns (uint160) {
    require(amount0 > 0 && amount1 > 0, "Amounts > 0");

    uint256 ratioX192 = FullMath.mulDiv(amount1, uint256(1 << 192), amount0);

    uint256 sqrtPriceX96 = Math.sqrt(ratioX192) >> 96;

    require(sqrtPriceX96 >= 4295128739, "RATIO");
    require(sqrtPriceX96 < 1461446703485210103287273052203988822378723970342, "RATIO");

    return uint160(sqrtPriceX96);
}



function onERC721Received(
        address operator,
        address from,
        uint256 tokenId,
        bytes calldata data
    ) external pure override returns (bytes4) {
        return IERC721Receiver.onERC721Received.selector;
}
}
