# DK/400 Test Results

Test Date: 2026-01-18
Instance: http://192.168.20.19:8400
Tester: Claude (Playwright MCP)

## Sign-On Tests
| Test | Result | Notes |
|------|--------|-------|
| Sign-on screen displays | PASS | Shows all expected fields |
| Sign-on with valid credentials | PASS | QSECOFR/QSECOFR worked (forced password change) |
| Password change screen | PASS | Displayed when password expired |
| Password change submit | PASS | Changed to SECURITY successfully |
| Main menu after sign-on | PASS | Shows menu options 1-12, 90 |

## Command Tests
| Command | Result | Notes |
|---------|--------|-------|
| WRKACTJOB | PASS | Shows interactive sessions and batch jobs |
| WRKACTJOB opt 5 | PASS | Displays job details correctly |
| DSPSYSSTS | PASS | Shows CPU, memory, pools, subsystems |
| WRKSVC | PASS | Shows Docker containers |
| WRKUSRPRF | PASS | Lists user profiles |
| WRKUSRPRF opt 5 | PASS | Displays user details |
| WRKLIB | PASS | Lists libraries |
| WRKSYSVAL | PASS | Shows system values, paging works |
| WRKJOBSCDE | PASS | Shows job schedules |
| WRKJOBSCDE opt 2 | PASS | Displays schedule details |
| WRKQRY | PASS | Full query flow: schema > table > F5 run |
| WRKJRN | PASS | Shows audit journal |
| WRKDTAARA | PASS | Lists data areas |
| DSPLOG | PASS | Shows system log |
| WRKHLTH | PASS | Shows container health checks |
| WRKBKP | PASS | Shows backup jobs |
| WRKBKP opt 5 | PASS | Displays backup details |
| WRKALR | PASS | Shows alerts |
| WRKALR opt 5 | PASS | Displays alert details |
| WRKNETDEV | PASS | Shows network devices |
| WRKNETDEV opt 5 | PASS | Displays device details |
| SBMJOB | PASS | Shows submit job form with Celery tasks |
| WRKJOBD | PASS | Lists job descriptions |
| WRKJOBD opt 2 | PASS | Displays job description details |
| WRKOUTQ | PASS | Shows output queues |
| WRKOUTQ opt 5 | PASS | Shows spooled files screen |
| WRKAUTL | PASS | Lists authorization lists |
| WRKSBSD | PASS | Lists subsystem descriptions |
| CRTUSRPRF | PASS | Shows create user profile form |
| CRTLIB | PASS | Shows create library form |
| CRTDTAARA | PASS | Shows create data area form |
| CRTJRNRCV | PASS | Shows create journal receiver form |
| CRTJRN | PASS | Shows create journal form |
| DSPJRN | PASS | Shows display journal prompt |
| STRJRNPF | PASS | Shows start journal PF form |
| WRKJOBQ | PASS | Shows job queues |
| WRKMSGQ | FIXED | Was crashing with 'queue_type' KeyError - fixed delivery field |

## Bugs Found and Fixed

### Bug 1: WRKMSGQ KeyError
- **Error:** `KeyError: 'queue_type'`
- **Cause:** Screen code accessed `q['queue_type']` but database returns `q['delivery']`
- **Fix:** Changed screens.py line 4615 to use `q.get('delivery', '*HOLD')`
- **Status:** FIXED

## Function Key Tests
| Key | Screen | Result | Notes |
|-----|--------|--------|-------|
| F3 | Various | PASS | Exits to previous screen |
| F5 | Various | PASS | Refreshes data |
| F12 | Various | PASS | Cancels and returns |
| PageDown | Lists | PASS | Scrolls down in long lists |
| PageUp | Lists | PASS | Scrolls up in long lists |

## Summary
- **Total Commands Tested:** 37
- **Passed:** 37
- **Failed:** 0 (1 fixed during testing)
- **Overall Status:** PASS
