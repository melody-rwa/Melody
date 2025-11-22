# MELODY Platform Document

## 1. Background & Objectives
This project aims to build a music NFT and liquidity management platform based on blockchain technology (EVM-compatible chain).

**Core Objectives:**
- **Artists** can create a **MusicSeries** and issue a corresponding **MemeToken** for each song.
- **Users** participate and support music creation via **Subscription**.
- The platform utilizes **PancakeswapV3 LP NFTs** as liquidity tools. Tokens contributed by users are pooled into liquidity and split into shares via the **FractionalVaultV3**:
  - **User Share:** 50%
  - **Artist Share:** 30% (Linear Vesting)
  - **Platform Share:** 20% total (3% Redeemable; 17% Non-redeemable/Locked, only fees can be withdrawn)
- Fees generated from liquidity are allocated to all parties and support anytime claiming.
- The artist's share is linearly unlocked via the **VestingVault**, allowing them to gradually redeem LP tokens and withdraw underlying assets.

## 2. System Participants
- **Artist**: Registers and creates Music Series and songs; receives 30% liquidity share (subject to linear vesting).
- **User**: Registers and participates via subscription; receives 50% liquidity share.
- **Platform**: Receives a 20% liquidity share, split as follows:
  - 3% Redeemable
  - 17% Locked (only fees can be withdrawn)
- **Platform Admin (Admin)**: Manages system contract upgrades and parameter configuration.
- **Platform MultiSig**: Receives platform shares and acts as the key permission controller.

## 3. Functional Requirements

### 3.1 User & Artist Registration
- **User Registration**: Register `profileCid` (`UserRegistry`).
- **Artist Registration**: Register `profileCid` and level (`ArtistRegistry`).
- **Queries**: Query User/Artist information, batch retrieval, and total count statistics.

### 3.2 Music Factory & Series Management (`MusicFactory`, `MusicSeries`)
- **Invitation Code System**:
  - Platform generates invitation codes; Artists use codes to create a Music Series.
  - Codes become invalid after consumption.
- **Music Series (MusicSeries)**:
  - Artists create series (via Factory).
  - Create songs under the factory pattern (`createMusic`).
  - Each song corresponds to an independent MemeToken (BEP-20).
- **Song Management**:
  - Record song metadata (IPFS CID, target raise amount, raise duration, etc.).
  - User subscription funds (payable).
  - Support for Whitelist priority subscription.
  - Call `finalize` after the fundraising period ends to enter the liquidity management flow.

### 3.3 Liquidity Management (`LPManagerV3`)
- Receive user-contributed `token0`/`token1` from `MusicSeries`.
- Call `Pancakeswap V3 PositionManager.mint` to create the LP NFT.
- Transfer the NFT to `FractionalVaultV3`.
- Trigger liquidity distribution:
  - **User**: 50%
  - **Artist**: 30% (Deposited into `VestingVault`)
  - **Platform MultiSig**: 3%
  - **Platform Fee Account**: 17%
- **Event**: `LiquidityAddedAndFinalized`.

### 3.4 Liquidity Share Custody & Allocation (`FractionalVaultV3`)
- Custody the LP NFT corresponding to each `songId`.
- Record allocation results (`DistributionInfo`).
- Manage User/Platform liquidity shares (`userLiquidity`).
- Collect fees (`collectFees`).
- User fee claiming (`claim`).
- User LP redemption (`redeem`).
  - **Restriction**: The Platform 17% address is prohibited from redeeming.
- **Query Functions**:
  - `getDistribution`, `getDistributions`, `getDistributionsByArtist`.
  - `getPendingForUser` (Estimate pending fees).

### 3.5 Artist Share Vesting (`VestingVault`)
- Record Vesting information for each song.
- **Linear Vesting Formula**:
  - `vestedAmount = totalAllocated * (timeElapsed / duration)`
  - `releasableAmount = vestedAmount - released`
- **Artist Operations**:
  - Claim Fees (`claimFees`).
  - Redeem unlocked LP (`redeem`).
- **Platform Calls**:
  - `FractionalVaultV3` calls `addVesting` upon `finalizeDistribution`.

### 3.6 MemeToken
- Specific BEP-20 token for each song.
- Standard BEP-20 interface.
- Used to represent participating user equity.

---

## 4. Non-Functional Requirements

### 4.1 Security
- All core contracts must use `AccessControlUpgradeable` for permission management.
- Critical platform parameters (e.g., MultiSig address, PositionManager address) are modifiable only by **Admin**.
- Reentrancy protection (`nonReentrant` modifier) applied to `claim` / `redeem`.
- **VestingVault**: When calling `redeem`, update `released` state *before* calling external contracts to prevent reentrancy attacks.

### 4.2 Upgradeability
- All core contracts inherit `UUPSUpgradeable` to support contract upgrades.
- Only the `UPGRADER_ROLE` possesses upgrade permissions (typically the Platform MultiSig).

### 4.3 Testing Requirements
- Unit tests written using **Foundry**, covering:
  - Allocation logic (User/Artist/Platform).
  - Fee collection and distribution.
  - Artist vesting linear unlocking.
  - LP redemption processes.
  - Edge cases (invalid parameters, insufficient permissions, duplicate calls, etc.).

---

## 5. Business Flow (End-to-End)

1. **Registration & Creation**
   - Artist registers in `ArtistRegistry`.
   - User registers in `UserRegistry`.
   - Platform issues an Invitation Code.
   - Artist uses the code to create a `MusicSeries` via `MusicFactory`.

2. **Song Creation & Fundraising**
   - Artist creates a song in `MusicSeries` (`createSong`).
   - Users subscribe (`subscribe`).
   - Fundraising period ends; `finalize` is called.

3. **Liquidity Addition & Allocation**
   - `MusicSeries` calls `LPManagerV3.addLiquidityAndFinalizeV3`.
   - `LPManagerV3` calls Pancakeswap V3 to create the LP NFT.
   - NFT is transferred to `FractionalVaultV3`.
   - `FractionalVaultV3` executes allocation and calls `VestingVault` to add the artist's share.

4. **Fees & Redemption**
   - Users/Platform call `FractionalVaultV3.claim` to retrieve fees.
   - Artists call `VestingVault.claimFees` to retrieve fees.
   - Users call `FractionalVaultV3.redeem` to redeem LP shares.
   - Artists call `VestingVault.redeem` to redeem LP shares based on the unlocked ratio.

---

## 6. Permissions & Roles Matrix

| Role | Permission Description |
|------|------------------------|
| **Admin** | Modify critical contract parameters, upgrade contracts, manage role registration. |
| **Upgrader** | Execute UUPS upgrades. |
| **MusicFactory** | Authorize MusicSeries, issue invitation codes. |
| **MusicSeries** | Interact with users on behalf of artists, call LPManager. |
| **LPManagerV3** | Mint LP NFTs, call `FractionalVault.finalizeDistribution`. |
| **FractionalVaultV3** | Allocate shares, collect/distribute fees, redeem liquidity. |
| **VestingVault** | Manage vesting unlocking for artist shares. |
| **Platform MultiSig (3%)** | Can redeem liquidity and claim fees. |
| **Platform Fee MultiSig (17%)** | **Prohibited from redeeming**; can only claim fees. |
| **User** | Subscribe, claim fees, redeem liquidity. |
| **Artist** | Create songs, claim vesting fees, redeem vesting LP. |

---

## 7. Exceptions & Error Scenarios

- **Registration**
  - User attempts update without registration -> `revert`.
  - Artist already registered attempts `register` again -> `revert`.
- **MusicFactory**
  - Use of invalid invitation code -> `revert`.
  - Illegal parameters during song creation -> `revert`.
- **LPManagerV3**
  - Total contributions equal 0 -> `revert`.
  - Call from an unregistered `MusicSeries` -> `revert`.
- **FractionalVaultV3**
  - Duplicate call to `finalizeDistribution` -> `revert`.
  - `platformFeeMultiSig` attempts `redeem` -> `revert`.
  - `liquidityToRedeem` is 0 -> `revert`.
- **VestingVault**
  - Non-artist calls `claimFees`/`redeem` -> `revert`.
  - `redeem` called with no releasable share -> `revert`.
- **Permissions**
  - Non-Admin attempts to modify critical parameters -> `revert`.
  - Non-Upgrader calls `_authorizeUpgrade` -> `revert`.
