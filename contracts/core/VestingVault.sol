// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

/*
VestingVault (updated)

- Singleton vesting vault that manages vesting for all songs' artist shares.
- Works together with FractionalVaultV3 which:
  * stores LP bookkeeping
  * on finalizeDistribution stores artist share under vestingVault address
  * FractionalVaultV3.claim / redeem transfer tokens to VestingVault and return token addresses & amounts

Assumptions:
- FractionalVaultV3.claim(songId) transfers owed fee tokens to this contract and returns (token0, token1, amt0, amt1)
- FractionalVaultV3.redeem(songId, amount) transfers underlying token0/token1 for `amount` LP-units to this contract and returns (token0, token1, amt0, amt1)
*/

import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/ReentrancyGuardUpgradeable.sol";

import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

interface IFractionalVaultV3 {
    // Now normalized to return token addresses + amounts
    function claim(uint256 songId) external returns (address token0, address token1, uint256 amount0, uint256 amount1);
    function redeem(uint256 songId, uint128 liquidityToRedeem,uint256 amount0Min,uint256 amount1Min) external returns (address token0, address token1, uint256 amount0, uint256 amount1);
}

contract VestingVault is Initializable, AccessControlUpgradeable, UUPSUpgradeable, ReentrancyGuardUpgradeable {
    using SafeERC20 for IERC20;

    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    bytes32 public constant MANAGER_ROLE  = keccak256("MANAGER_ROLE"); // assigned to FractionalVaultV3

    struct VestingInfo {
        address artist;
        uint128 totalAllocated; // total LP-units allocated for artist (base units used by FractionalVault)
        uint128 released;       // LP-units already redeemed/released
        uint256 startTime;
        uint256 duration;
    }

    // songId -> vesting info
    mapping(uint256 => VestingInfo) public vestings;

    // references
    address public fractionalVault; // FractionalVaultV3 address
    address public admin;

    event VestingAdded(uint256 indexed songId, address indexed artist, uint256 amount, uint256 start, uint256 duration);
    event FeesClaimed(uint256 indexed songId, address indexed artist, address token0, address token1, uint256 amount0, uint256 amount1);
    event LPRedeemed(uint256 indexed songId, address indexed artist, uint256 lpUnits, address token0, address token1, uint256 amount0, uint256 amount1);

    /// @notice initialize VestingVault singleton
    function initialize(address _admin, address _fractionalVault,address _upgrader) public initializer {
        require(_admin != address(0) && _upgrader!=address(0), "address error");
        require(_fractionalVault != address(0), "fv=0");

        __AccessControl_init();
        __UUPSUpgradeable_init();
        __ReentrancyGuard_init();

        admin = _admin;
        fractionalVault = _fractionalVault;

        _grantRole(DEFAULT_ADMIN_ROLE, _admin);
        _grantRole(UPGRADER_ROLE, _upgrader);
        // allow FractionalVaultV3 to add vestings
        _grantRole(MANAGER_ROLE, _fractionalVault);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    // --- Manager: FractionalVaultV3 calls this to register vesting for artist-share ---
    function addVesting(
        uint256 songId,
        address artist,
        uint128 amount,      // LP-units assigned to artist (e.g. 30% portion in LP units)
        uint256 start,
        uint256 duration
    ) external onlyRole(MANAGER_ROLE) {
        require(artist != address(0), "artist=0");
        require(amount > 0, "amount=0");
        require(duration > 0, "duration=0");
        require(vestings[songId].artist == address(0), "exists");

        vestings[songId] = VestingInfo({
            artist: artist,
            totalAllocated: amount,
            released: 0,
            startTime: start,
            duration: duration
        });

        emit VestingAdded(songId, artist, amount, start, duration);
    }

    // --- Artist functions ---

    /// @notice Claim accumulated fees for a song (fees were recorded under vestingVault in FractionalVaultV3)
    /// Flow:
    /// - Call FractionalVaultV3.claim(songId), which transfers fee tokens to this contract and returns token addresses + amounts.
    /// - Forward token amounts to the artist.
    function claimFees(uint256 songId) external nonReentrant {
        VestingInfo storage v = vestings[songId];
        require(v.artist == msg.sender, "not artist");

        (address token0, address token1, uint256 amt0, uint256 amt1) = IFractionalVaultV3(fractionalVault).claim(songId);

        // forward ERC20 tokens if present
        if (amt0 > 0 && token0 != address(0)) {
            IERC20(token0).safeTransfer(v.artist, amt0);
        }
        if (amt1 > 0 && token1 != address(0)) {
            IERC20(token1).safeTransfer(v.artist, amt1);
        }

        emit FeesClaimed(songId, v.artist, token0, token1, amt0, amt1);
    }

    /// @notice Redeem artist's vested LP-units (only the releasable amount)
    /// Flow:
    /// - Check releasable LP-units by linear vesting schedule
    /// - Call FractionalVaultV3.redeem(songId, units) which transfers token0/token1 to this contract and returns them
    /// - Forward tokens to artist and update released amount
    function redeem(uint256 songId) external nonReentrant {
        VestingInfo storage v = vestings[songId];
        require(v.artist == msg.sender, "not artist");

        uint128 vested = _vestedAmount(v);
        require(vested > v.released, "nothing vested");
        uint128 releasable = vested - v.released;
        require(releasable > 0, "no releasable");

        // update released before external call to prevent reentrancy issues
        v.released += releasable;

        (address token0, address token1, uint256 amt0, uint256 amt1) = IFractionalVaultV3(fractionalVault).redeem(songId, releasable,0,0);

        // forward received tokens to artist
        if (amt0 > 0 && token0 != address(0)) {
            IERC20(token0).safeTransfer(v.artist, amt0);
        }
        if (amt1 > 0 && token1 != address(0)) {
            IERC20(token1).safeTransfer(v.artist, amt1);
        }

        emit LPRedeemed(songId, v.artist, releasable, token0, token1, amt0, amt1);
    }

    // --- Views ---

    /// @notice Returns total vested units for a vesting info (amount that should be vested so far)
    function vestedAmount(uint256 songId) external view returns (uint128) {
        return _vestedAmount(vestings[songId]);
    }

    /// @notice Returns releasable units (vested - released)
    function releasableAmount(uint256 songId) external view returns (uint128) {
        VestingInfo memory v = vestings[songId];
        uint128 vested = _vestedAmount(v);
        if (vested <= v.released) return 0;
        return vested - v.released;
    }
    function getVesting(uint256 songId) external view returns (VestingInfo memory) {
        return vestings[songId];
    }

    function _vestedAmount(VestingInfo memory v) internal view returns (uint128) {
        if (v.artist == address(0)) return 0;
        if (block.timestamp <= v.startTime) return 0;
        uint256 end = v.startTime + v.duration;
        if (block.timestamp >= end) return v.totalAllocated;
        uint256 elapsed = block.timestamp - v.startTime;
        uint256 vested = (uint256(v.totalAllocated) * elapsed) / v.duration;
        return uint128(vested);
    }

    // admin setter if FractionalVault address changes
    function setFractionalVault(address _fv) external onlyRole(UPGRADER_ROLE) {
        require(_fv != address(0), "zero");
        // revoke old manager role and grant new manager role (optional)
        address old = fractionalVault;
        fractionalVault = _fv;
        // grant manager role to new fractional vault
        _grantRole(MANAGER_ROLE, _fv);
        if (old != address(0)) {
            // optionally revoke role from old
            _revokeRole(MANAGER_ROLE, old);
        }
    }

    receive() external payable {}
}
